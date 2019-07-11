module github.com/danielqsj/kafka_exporter

go 1.12

require (
	github.com/DataDog/zstd v1.4.0 // indirect
	github.com/Shopify/sarama v1.23.0
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/golang/protobuf v1.3.2 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/krallistic/kazoo-go v0.0.0-20170526135507-a15279744f4e
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.6.0
	github.com/prometheus/procfs v0.0.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20190706150252-9beb055b7962
	github.com/samuel/go-zookeeper v0.0.0-20180130194729-c4fab1ac1bec // indirect
	github.com/sirupsen/logrus v1.4.2 // indirect
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4 // indirect
	golang.org/x/net v0.0.0-20190628185345-da137c7871d7 // indirect
	golang.org/x/sys v0.0.0-20190710143415-6ec70d6a5542 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6
	gopkg.in/jcmturner/goidentity.v3 v3.0.0 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
)

replace github.com/Shopify/sarama => github.com/evazca/sarama v0.0.0-20190711085333-61eeb6384882
