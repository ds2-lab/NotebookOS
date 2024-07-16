module github.com/zhangjyr/distributed-notebook

go 1.21

toolchain go1.21.5

replace github.com/go-zeromq/zmq4 => ../zmq4

require (
	github.com/charmbracelet/lipgloss v0.11.0
	github.com/colinmarc/hdfs/v2 v2.4.0
	github.com/elliotchance/orderedmap/v2 v2.2.0
	github.com/fsnotify/fsnotify v1.5.1
	github.com/go-zeromq/zmq4 v0.16.0
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/grpc-opentracing v0.0.0-20180507213350-8e809c8a8645
	github.com/hashicorp/consul/api v1.28.2
	github.com/hashicorp/yamux v0.1.1
	github.com/mason-leap-lab/go-utils v1.3.3
	github.com/onsi/ginkgo/v2 v2.15.0
	github.com/onsi/gomega v1.32.0
	github.com/opentracing-contrib/go-stdlib v1.0.0
	github.com/opentracing/opentracing-go v1.1.0
	github.com/orcaman/concurrent-map/v2 v2.0.1
	github.com/petermattis/goid v0.0.0-20240607163614-bb94eb51e7a7
	github.com/pkg/errors v0.9.1
	github.com/scusemua/gopy v0.4.3
	github.com/shopspring/decimal v1.3.1
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/zhangjyr/gopy v0.4.2
	github.com/zhangjyr/hashmap v1.0.2
	go.etcd.io/etcd/client/pkg/v3 v3.5.2
	go.etcd.io/etcd/raft/v3 v3.5.2
	go.etcd.io/etcd/server/v3 v3.5.2
	go.uber.org/mock v0.4.0
	go.uber.org/zap v1.21.0
	golang.org/x/exp v0.0.0-20230817173708-d852ddb80c63
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.2
	k8s.io/api v0.29.3
	k8s.io/apimachinery v0.29.3
	k8s.io/client-go v0.29.1
	k8s.io/klog/v2 v2.110.1
	k8s.io/kube-scheduler v0.29.3
	k8s.io/kubelet v0.29.1
	nhooyr.io/websocket v1.8.10
)

require (
	github.com/aymanbagabas/go-osc52/v2 v2.0.1 // indirect
	github.com/charmbracelet/x/ansi v0.1.1 // indirect
	github.com/go-zeromq/goczmq/v4 v4.2.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/lucasb-eyer/go-colorful v1.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/muesli/termenv v0.15.2 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	golang.org/x/sync v0.7.0 // indirect
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bytedance/sonic v1.11.3 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20230717121745-296ad89f973d // indirect
	github.com/chenzhuoyu/iasm v0.9.1 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dchest/siphash v1.1.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/emicklei/go-restful/v3 v3.11.0 // indirect
	github.com/fatih/color v1.14.1 // indirect
	github.com/gabriel-vasile/mimetype v1.4.3 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/gin-gonic/contrib v0.0.0-20221130124618-7e01895a63f2
	github.com/gin-gonic/gin v1.9.1
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.19.0 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/pprof v0.0.0-20210720184732-4bb14d4b1be1 // indirect
	github.com/gookit/config/v2 v2.1.2 // indirect
	github.com/gookit/goutil v0.5.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.1.0
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-hclog v1.5.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.7 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pelletier/go-toml/v2 v2.2.1 // indirect
	github.com/prometheus/client_golang v1.16.0 // indirect
	github.com/prometheus/client_model v0.4.0 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.10.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/etcd/api/v3 v3.5.2 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.2 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/arch v0.7.0 // indirect
	golang.org/x/crypto v0.24.0 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/oauth2 v0.18.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/term v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	golang.org/x/time v0.3.0
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240624140628-dc46fd24d27d // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/kube-openapi v0.0.0-20231010175941-2dd684a91f00 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
