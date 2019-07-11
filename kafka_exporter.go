package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	loggo "github.com/jeanphorn/log4go"
	"github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "kafka"
	clientID  = "kafka_exporter"
)

var (
	clusterBrokers                     *prometheus.Desc
	topicPartitions                    *prometheus.Desc
	topicCurrentOffset                 *prometheus.Desc
	topicOldestOffset                  *prometheus.Desc
	topicPartitionLeader               *prometheus.Desc
	topicPartitionReplicas             *prometheus.Desc
	topicPartitionInSyncReplicas       *prometheus.Desc
	topicPartitionUsesPreferredReplica *prometheus.Desc
	topicUnderReplicatedPartition      *prometheus.Desc
	consumergroupCurrentOffset         *prometheus.Desc
	consumergroupCurrentOffsetSum      *prometheus.Desc
	consumergroupLag                   *prometheus.Desc
	consumergroupLagSum                *prometheus.Desc
	consumergroupLagZookeeper          *prometheus.Desc
	consumergroupMembers               *prometheus.Desc
)

var (
	token      =  ""
	commonLogger        *loggo.Filter
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	groupFilter             *regexp.Regexp
	mu                      sync.Mutex
	useZooKeeperLag         bool
	zookeeperClient         *kazoo.Kazoo
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
}

type kafkaOpts struct {
	uri                      []string
	useSASL                  bool
	useSASLHandshake         bool
	saslUsername             string
	saslPassword             string
	useTLS                   bool
	tlsCAFile                string
	tlsCertFile              string
	tlsKeyFile               string
	tlsInsecureSkipTLSVerify bool
	kafkaVersion             string
	useZooKeeperLag          bool
	uriZookeeper             []string
	labels                   string
	metadataRefreshInterval  string
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts kafkaOpts, topicFilter string, groupFilter string) (*Exporter, error) {
	var zookeeperClient *kazoo.Kazoo
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		return nil, err
	}
	config.Version = kafkaVersion

	if opts.useSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if opts.saslPassword != "" {
			config.Net.SASL.Password = opts.saslPassword
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := ioutil.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				commonLogger.Critical(err)
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			commonLogger.Critical(err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				commonLogger.Critical(err)
			}
		}
	}

	if opts.useZooKeeperLag {
		zookeeperClient, err = kazoo.NewKazoo(opts.uriZookeeper, nil)
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		commonLogger.Error("Cannot parse metadata refresh interval")
		panic(err)
	}

	config.Metadata.RefreshFrequency = interval

	client, err := sarama.NewClient(opts.uri, config)

	if err != nil {
		commonLogger.Error("Error Init Kafka Client")
		panic(err)
	}
	commonLogger.Info("Done Init Clients")

	// Init our exporter.
	return &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile(topicFilter),
		groupFilter:             regexp.MustCompile(groupFilter),
		useZooKeeperLag:         opts.useZooKeeperLag,
		zookeeperClient:         zookeeperClient,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
	}, nil
}

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- clusterBrokers
	ch <- topicCurrentOffset
	ch <- topicOldestOffset
	ch <- topicPartitions
	ch <- topicPartitionLeader
	ch <- topicPartitionReplicas
	ch <- topicPartitionInSyncReplicas
	ch <- topicPartitionUsesPreferredReplica
	ch <- topicUnderReplicatedPartition
	ch <- consumergroupCurrentOffset
	ch <- consumergroupCurrentOffsetSum
	ch <- consumergroupLag
	ch <- consumergroupLagZookeeper
	ch <- consumergroupLagSum
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var wg = sync.WaitGroup{}
	ch <- prometheus.MustNewConstMetric(
		clusterBrokers, prometheus.GaugeValue, float64(len(e.client.Brokers())),
	)

	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		commonLogger.Info("Refreshing client metadata")

		if err := e.client.RefreshMetadata(); err != nil {
			commonLogger.Error("Cannot refresh topics, using cached data: %v", err)
		}

		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	topics, err := e.client.Topics()
	if err != nil {
		commonLogger.Error("Cannot get topics: %v", err)
		return
	}

	getTopicMetrics := func(topic string) {
		defer wg.Done()
		if e.topicFilter.MatchString(topic) {
			partitions, err := e.client.Partitions(topic)
			if err != nil {
				commonLogger.Error("Cannot get partitions of topic %s: %v", topic, err)
				return
			}
			ch <- prometheus.MustNewConstMetric(
				topicPartitions, prometheus.GaugeValue, float64(len(partitions)), topic,
			)
			e.mu.Lock()
			offset[topic] = make(map[int32]int64, len(partitions))
			e.mu.Unlock()
			for _, partition := range partitions {
				broker, err := e.client.Leader(topic, partition)
				if err != nil {
					commonLogger.Error("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionLeader, prometheus.GaugeValue, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				currentOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					commonLogger.Error("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
				} else {
					e.mu.Lock()
					offset[topic][partition] = currentOffset
					e.mu.Unlock()
					ch <- prometheus.MustNewConstMetric(
						topicCurrentOffset, prometheus.GaugeValue, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				oldestOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetOldest)
				if err != nil {
					commonLogger.Error("Cannot get oldest offset of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicOldestOffset, prometheus.GaugeValue, float64(oldestOffset), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				replicas, err := e.client.Replicas(topic, partition)
				if err != nil {
					commonLogger.Error("Cannot get replicas of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionReplicas, prometheus.GaugeValue, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				inSyncReplicas, err := e.client.InSyncReplicas(topic, partition)
				if err != nil {
					commonLogger.Error("Cannot get in-sync replicas of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionInSyncReplicas, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				if broker != nil && replicas != nil && len(replicas) > 0 && broker.ID() == replicas[0] {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
					)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				if replicas != nil && inSyncReplicas != nil && len(inSyncReplicas) < len(replicas) {
					ch <- prometheus.MustNewConstMetric(
						topicUnderReplicatedPartition, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
					)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicUnderReplicatedPartition, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				if e.useZooKeeperLag {
					ConsumerGroups, err := e.zookeeperClient.Consumergroups()

					if err != nil {
						commonLogger.Error("Cannot get consumer group %v", err)
					}

					for _, group := range ConsumerGroups {
						offset, _ := group.FetchOffset(topic, partition)
						if offset > 0 {

							consumerGroupLag := currentOffset - offset
							ch <- prometheus.MustNewConstMetric(
								consumergroupLagZookeeper, prometheus.GaugeValue, float64(consumerGroupLag), group.Name, topic, strconv.FormatInt(int64(partition), 10),
							)
						}
					}
				}
			}
		}
	}

	for _, topic := range topics {
		wg.Add(1)
		go getTopicMetrics(topic)
	}

	wg.Wait()

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			commonLogger.Error("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			commonLogger.Error("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			commonLogger.Error("Cannot get describe groups: %v", err)
			return
		}
		for _, group := range describeGroups.Groups {
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 5}
			ch <- prometheus.MustNewConstMetric(
				consumergroupMembers, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
			)
			if offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest); err != nil {
				commonLogger.Error("Cannot get offset of group %s: %v", group.GroupId, err)
			} else {
				for topic, partitions := range offsetFetchResponse.Blocks {
					// If the topic is not consumed by that consumer group, skip it
					topicConsumed := false
					for _, offsetFetchResponseBlock := range partitions {
						// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
						if offsetFetchResponseBlock.Offset != -1 {
							topicConsumed = true
							break
						}
					}
					if topicConsumed {
						var currentOffsetSum int64
						var lagSum int64
						for partition, offsetFetchResponseBlock := range partitions {
							err := offsetFetchResponseBlock.Err
							if err != sarama.ErrNoError {
								commonLogger.Error("Error for  partition %d :%v", partition, err.Error())
								continue
							}
							currentOffset := offsetFetchResponseBlock.Offset
							currentOffsetSum += currentOffset
							ch <- prometheus.MustNewConstMetric(
								consumergroupCurrentOffset, prometheus.GaugeValue, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
							)
							e.mu.Lock()
							if offset, ok := offset[topic][partition]; ok {
								// If the topic is consumed by that consumer group, but no offset associated with the partition
								// forcing lag to -1 to be able to alert on that
								var lag int64
								if offsetFetchResponseBlock.Offset == -1 {
									lag = -1
								} else {
									lag = offset - offsetFetchResponseBlock.Offset
									lagSum += lag
								}
								ch <- prometheus.MustNewConstMetric(
									consumergroupLag, prometheus.GaugeValue, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
								)
							} else {
								commonLogger.Error("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
							}
							e.mu.Unlock()
						}
						ch <- prometheus.MustNewConstMetric(
							consumergroupCurrentOffsetSum, prometheus.GaugeValue, float64(currentOffsetSum), group.GroupId, topic,
						)
						ch <- prometheus.MustNewConstMetric(
							consumergroupLagSum, prometheus.GaugeValue, float64(lagSum), group.GroupId, topic,
						)
					}
				}
			}
		}
	}

	if len(e.client.Brokers()) > 0 {
		for _, broker := range e.client.Brokers() {
			wg.Add(1)
			go getConsumerGroupMetrics(broker)
		}
		wg.Wait()
	} else {
		commonLogger.Error("No valid broker, cannot get consumer group metrics")
	}
}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))
}

func main() {
	loggo.LoadConfiguration("./log.json")
	commonLogger = loggo.LOGGER("common")
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9308").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		topicFilter   = kingpin.Flag("topic.filter", "Regex that determines which topics to collect.").Default(".*").String()
		groupFilter   = kingpin.Flag("group.filter", "Regex that determines which consumer groups to collect.").Default(".*").String()
		logSarama     = kingpin.Flag("log.enable-sarama", "Turn on Sarama logging.").Default("false").Bool()

		opts = kafkaOpts{}
	)
	uris := kingpin.Flag("kafka.server", "Address (host:port) of Kafka server.").Default("kafka:9092").String()
	kingpin.Flag("sasl.enabled", "Connect using SASL/PLAIN.").Default("false").BoolVar(&opts.useSASL)
	kingpin.Flag("sasl.handshake", "Only set this to false if using a non-Kafka SASL proxy.").Default("true").BoolVar(&opts.useSASLHandshake)
	kingpin.Flag("sasl.username", "SASL user name.").Default("").StringVar(&opts.saslUsername)
	kingpin.Flag("sasl.password", "SASL user password.").Default("").StringVar(&opts.saslPassword)
	kingpin.Flag("tls.enabled", "Connect using TLS.").Default("false").BoolVar(&opts.useTLS)
	kingpin.Flag("tls.ca-file", "The optional certificate authority file for TLS client authentication.").Default("").StringVar(&opts.tlsCAFile)
	kingpin.Flag("tls.cert-file", "The optional certificate file for client authentication.").Default("").StringVar(&opts.tlsCertFile)
	kingpin.Flag("tls.key-file", "The optional key file for client authentication.").Default("").StringVar(&opts.tlsKeyFile)
	kingpin.Flag("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure.").Default("false").BoolVar(&opts.tlsInsecureSkipTLSVerify)
	kingpin.Flag("kafka.version", "Kafka broker version").Default(sarama.V1_0_0_0.String()).StringVar(&opts.kafkaVersion)
	kingpin.Flag("use.consumelag.zookeeper", "if you need to use a group from zookeeper").Default("false").BoolVar(&opts.useZooKeeperLag)
	kingpin.Flag("zookeeper.server", "Address (hosts) of zookeeper server.").Default("localhost:2181").StringsVar(&opts.uriZookeeper)
	kingpin.Flag("kafka.labels", "Kafka cluster name").Default("").StringVar(&opts.labels)
	kingpin.Flag("refresh.metadata", "Metadata refresh interval").Default("30s").StringVar(&opts.metadataRefreshInterval)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	commonLogger.Info("Starting kafka_exporter", version.Info())
	commonLogger.Info("Build context", version.BuildContext())
	opts.uri = strings.Split(*uris, ",")
	labels := make(map[string]string)

	// Protect against empty labels
	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	clusterBrokers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "brokers"),
		"Number of Brokers in the Kafka Cluster.",
		nil, labels,
	)
	topicPartitions = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partitions"),
		"Number of partitions for this Topic",
		[]string{"topic"}, labels,
	)
	topicCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_current_offset"),
		"Current Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)
	topicOldestOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_oldest_offset"),
		"Oldest Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionLeader = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader"),
		"Leader Broker ID of this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_replicas"),
		"Number of Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionInSyncReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_in_sync_replica"),
		"Number of In-Sync Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionUsesPreferredReplica = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader_is_preferred"),
		"1 if Topic/Partition is using the Preferred Broker",
		[]string{"topic", "partition"}, labels,
	)

	topicUnderReplicatedPartition = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_under_replicated_partition"),
		"1 if Topic/Partition is under Replicated",
		[]string{"topic", "partition"}, labels,
	)

	consumergroupCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset"),
		"Current Offset of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupCurrentOffsetSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset_sum"),
		"Current Offset of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag"),
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupLagZookeeper = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroupzookeeper", "lag_zookeeper"),
		"Current Approximate Lag(zookeeper) of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)

	consumergroupLagSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag_sum"),
		"Current Approximate Lag of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	if *logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, *topicFilter, *groupFilter)
	if err != nil {
		commonLogger.Critical(err)
	}
	defer exporter.client.Close()
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + *metricsPath + `'>Metrics</a></p>
	        </body>
	        </html>`))
	})
	initAgentApi(opts)
	commonLogger.Info("Listening on", *listenAddress)

	//diy code don't merge to kafka_exporter
	initWriteTask(*listenAddress + *metricsPath)

	commonLogger.Critical(http.ListenAndServe(*listenAddress, nil))

}

//diy code don't merge to kafka_exporter
func initKafkaAdmin(opts kafkaOpts) (sarama.ClusterAdmin,error) {
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		return nil,err
	}
	config.Version = kafkaVersion

	if opts.useSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if opts.saslPassword != "" {
			config.Net.SASL.Password = opts.saslPassword
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := ioutil.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				commonLogger.Critical(err)
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			commonLogger.Critical(err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				commonLogger.Critical(err)
			}
		}
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		commonLogger.Error("Cannot parse metadata refresh interval")
		return nil,err
	}

	config.Metadata.RefreshFrequency = interval

	client, err := sarama.NewClusterAdmin(opts.uri, config)
	if err != nil {
		commonLogger.Error("Error Init Kafka admin Client")
		return nil,err
	}
	commonLogger.Info("Done Init admin Clients")
	return client,nil
}

func initAgentApi(opts kafkaOpts) {
	http.HandleFunc("/topic/create", func(w http.ResponseWriter, r *http.Request) {
		if prepareRequest(w, r) {
			adminClient,err := initKafkaAdmin(opts)
			if err != nil {
				commonLogger.Error("Error for init admin client %v", err.Error())
				w.WriteHeader(404)
				w.Write([]byte("Error for init admin client " + err.Error()))
			}
			defer adminClient.Close()
			postForm := r.PostForm
			if len(postForm["name"]) == 0 {
				w.WriteHeader(404)
				w.Write([]byte("topic name not passed"))
			}else {
				name := postForm["name"][0]
				numPartition := 5
				replicationFactor := 3
				if len(postForm["numPartition"]) != 0 {
					num,err := strconv.Atoi(postForm["numPartition"][0])
					if err == nil {
						numPartition = num
					}
				}
				if len(postForm["replicationFactor"]) != 0 {
					num,err := strconv.Atoi(postForm["replicationFactor"][0])
					if err == nil {
						replicationFactor = num
					}
				}
				topicDetail := &sarama.TopicDetail{
					NumPartitions: int32(numPartition),
					ReplicationFactor: int16(replicationFactor),
				}
				err := adminClient.CreateTopic(name,topicDetail,false)
				if err != nil {
					commonLogger.Error("Error for  create topic %v", err.Error())
					w.WriteHeader(404)
					w.Write([]byte("create topic error " + err.Error()))
				}else {
					w.WriteHeader(200)
					w.Write([]byte("ok"))
				}
			}
		}
	})
	http.HandleFunc("/topic/delete", func(w http.ResponseWriter, r *http.Request) {
		if prepareRequest(w, r) {
			adminClient,err := initKafkaAdmin(opts)
			if err != nil {
				commonLogger.Error("Error for init admin client %v", err.Error())
				w.WriteHeader(404)
				w.Write([]byte("Error for init admin client " + err.Error()))
			}
			defer adminClient.Close()
			postForm := r.PostForm
			if len(postForm["name"]) == 0 {
				w.WriteHeader(404)
				w.Write([]byte("topic name not passed"))
			}else {
				name := postForm["name"][0]
				err := adminClient.DeleteTopic(name)
				if err != nil {
					commonLogger.Error("Error for  delete topic %v", err.Error())
					w.WriteHeader(404)
					w.Write([]byte("delete topic error " + err.Error()))
				}else {
					w.WriteHeader(200)
					w.Write([]byte("ok"))
				}
			}
		}
	})
	http.HandleFunc("/topic/records/delete", func(w http.ResponseWriter, r *http.Request) {
		if prepareRequest(w, r) {
			adminClient,err := initKafkaAdmin(opts)
			if err != nil {
				commonLogger.Error("Error for init admin client %v", err.Error())
				w.WriteHeader(404)
				w.Write([]byte("Error for init admin client " + err.Error()))
			}
			defer adminClient.Close()
			postForm := r.PostForm
			if len(postForm["name"]) == 0 {
				w.WriteHeader(404)
				w.Write([]byte("topic name not passed"))
			}else {
				name := postForm["name"][0]
				if len(postForm["partitionOffsets"]) == 0 {
					w.WriteHeader(404)
					w.Write([]byte("partitionOffsets not passed"))
					return
				}
				partitionOffsetsStr := postForm["partitionOffsets"][0]
				partitionOffsets :=  make(map[int32]int64)
				for _,partitionOffsetStr := range strings.Split(partitionOffsetsStr,","){
					partitionOffsetArray := strings.Split(partitionOffsetStr,":")
					partition,err := strconv.Atoi(partitionOffsetArray[0])
					if err != nil {
						w.WriteHeader(404)
						w.Write([]byte("partitionOffset parse failed " + partitionOffsetsStr))
						return
					}
					offset,err := strconv.Atoi(partitionOffsetArray[1])
					if err != nil {
						w.WriteHeader(404)
						w.Write([]byte("partitionOffset parse failed " + partitionOffsetsStr))
						return
					}
					partitionOffsets[int32(partition)] = int64(offset)
				}
				if len(partitionOffsets) == 0 {
					w.WriteHeader(404)
					w.Write([]byte("partitionOffset parse failed " + partitionOffsetsStr))
					return
				}							
				commonLogger.Debug("param is %v" ,partitionOffsets )
				err := adminClient.DeleteRecords(name,partitionOffsets)
				if err != nil {
					commonLogger.Error("Error for  delete record %v", err.Error())
					w.WriteHeader(404)
					w.Write([]byte("delete record error " + err.Error()))
				}else {
					w.WriteHeader(200)
					w.Write([]byte("ok"))
				}
			}
		}
	})
	http.HandleFunc("/topic/createpartition", func(w http.ResponseWriter, r *http.Request) {
		if prepareRequest(w, r) {
			adminClient,err := initKafkaAdmin(opts)
			if err != nil {
				commonLogger.Error("Error for init admin client %v", err.Error())
				w.WriteHeader(404)
				w.Write([]byte("Error for init admin client " + err.Error()))
			}
			defer adminClient.Close()
			postForm := r.PostForm
			if len(postForm["name"]) == 0 {
				w.WriteHeader(404)
				w.Write([]byte("topic name not passed"))
			}else {
				name := postForm["name"][0]
				if len(postForm["count"]) == 0 {
					w.WriteHeader(404)
					w.Write([]byte("topic name not passed"))
					return
				}
				count,err := strconv.Atoi(postForm["count"][0])
				if err != nil {
					commonLogger.Error("Error for  count atoi %v", err.Error())
					w.WriteHeader(404)
					w.Write([]byte("count atoi error " + err.Error()))
					return
				}
				err = adminClient.CreatePartitions(name, int32(count), nil, false)
				if err != nil {
					commonLogger.Error("Error for  create partition %v", err.Error())
					w.WriteHeader(404)
					w.Write([]byte("create partition error" + err.Error()))
				}else {
					w.WriteHeader(200)
					w.Write([]byte("ok"))
				}
			}
		}
	})
}

func prepareRequest(w http.ResponseWriter, r *http.Request) bool {
	r.ParseForm()
	tokenParam := r.Form.Get("token")
	if token != tokenParam {
		w.WriteHeader(404)
		w.Write([]byte(`token not verfied` ))
		return false
	}
	return true
}

func initWriteTask(url string)  {
	url = "http://localhost"+url
	c := time.Tick(30 * time.Second)
	go func() {
		for {
			<-c
			writeZabbix(url)
		}
	}()

}

func writeZabbix(url string){
	resp, err := http.Get(url)
	if err != nil {
		commonLogger.Error("get url" + url +  " error: %v", err)
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		commonLogger.Error("get url " + url +  " error: %v" ,err)
		return
	}

	var result = parse(string(body))
	writeResult(result)
}

type zabbixResult struct {
	LastUpdateTime int64  `json:"lastUpdateTime"`
	DestList []dest  `json:"destList"`
}

type dest struct {
	QueueName string  `json:"queueName"`
	Count     int     `json:"count"`
	ConsumerCount int  `json:"consumerCount"`
}

func parse(body string) zabbixResult {
	//kafka_consumergroup_lag{consumergroup="newget",partition="1",topic="td"} 0
	//kafka_consumergroup_members{consumergroup="newget"} 0
	//queuename td-1-newget

	commonLogger.Debug("parse body is ",body)
	groupMemberMap := make(map[string]string)
	groupLags := make([][4]string,0)
	for _,line := range strings.Split(body,"\n"){
		if strings.HasPrefix(line,"kafka_consumergroup_lag{"){
			reg,err := regexp.Compile(`kafka_consumergroup_lag{consumergroup="(.+)",partition="(\d+)",topic="(.+)"} (\d+)`)
			if err == nil{
				result := reg.FindStringSubmatch(line)
				if len(result) > 0 {
					commonLogger.Debug("reg result is ",result)
					group := result[1]
					partition := result[2]
					topic := result[3]
					count := result[4]
					groupLags = append(groupLags, [4]string{group, topic, partition, count})
				}
			}
		}else if strings.HasPrefix(line,"kafka_consumergroup_members{"){
			reg,err := regexp.Compile(`kafka_consumergroup_members{consumergroup="(.+)"} (\d+)`)
			if err == nil{
				result := reg.FindStringSubmatch(line)
				if len(result) > 0 {
					commonLogger.Debug("reg result is ",result)
					group := result[1]
					count := result[2]
					groupMemberMap[group] = count
				}
			}
		}
	}
	result := zabbixResult{time.Now().Unix() * 1000 ,[]dest{}}
	for _,groupLag := range groupLags{
		commonLogger.Debug("grouplag is  ",groupLag)
		group := groupLag[0]
		var consumerCount,count int
		var err error
		if _,ok := groupMemberMap[group]; ok{
			if consumerCount,err = strconv.Atoi(groupMemberMap[group]);err != nil{
				continue
			}
		}else {
			consumerCount = 0
		}
		name := groupLag[1] +"-" +  groupLag[2] +"-" + group
		if count,err = strconv.Atoi(groupLag[3]);err != nil{
			continue
		}
		result.DestList = append(result.DestList,dest{name,count,consumerCount})
	}
	commonLogger.Debug("final result is  ",result)
	return result
}

func writeResult(result zabbixResult)  {
	jsonStr,err := json.Marshal(result)
	if err != nil{
		commonLogger.Error("json marshal error: %v " ,err)
		return
	}

	dirPath := "/wls/applogs/destview"
	_, err = os.Stat(dirPath)
	if err != nil && !os.IsExist(err) {
		err = os.MkdirAll(dirPath,os.ModePerm)
		if err != nil{
			commonLogger.Error("create destView file error: %v " ,err)
			return
		}
	}
	filePath := dirPath + "/destView.json"
	file,err := os.OpenFile(filePath,os.O_TRUNC | os.O_CREATE |os.O_RDWR , os.ModePerm)
	if err != nil {
		commonLogger.Error("open file error: %v" ,err)
		return
	}
	defer file.Close()
	if _,err := file.Write(jsonStr);err != nil{
		commonLogger.Error("write file error: %v" ,err)
	}
}
