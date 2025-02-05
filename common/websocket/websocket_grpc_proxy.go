package websocket

import (
	"fmt"
	"net"
	"net/http"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"nhooyr.io/websocket"
)

//
// Reference:
// - https://github.com/pojntfx/go-app-grpc-chat-backend/tree/master
//

type WebSocketProxyServer struct {
	log            logger.Logger
	stopChan       chan struct{} // Tell the server to stop listening.
	errorChan      chan error    // Report errors via this channel.
	connectionChan chan net.Conn
	server         *http.Server // The underlying HTTP server.
	address        string       // The address that the HTTP server will listen on.

}

func NewWebSocketProxyServer(address string) *WebSocketProxyServer {
	proxyServer := &WebSocketProxyServer{
		address:        address,
		stopChan:       make(chan struct{}),
		errorChan:      make(chan error, 1),
		connectionChan: make(chan net.Conn)}

	config.InitLogger(&proxyServer.log, proxyServer)

	return proxyServer
}

func (p *WebSocketProxyServer) Listen() (net.Listener, error) {
	p.log.Debug("Listening on tcp://%s", p.address)

	p.server = &http.Server{
		Handler: p,
	}

	lis, err := net.Listen("tcp", p.address)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(p.errorChan)

		// Serve until there's an error, at which point we send the error over the associated channel.
		p.errorChan <- p.server.Serve(lis)
	}()

	return p, nil
}

func (p *WebSocketProxyServer) ServeHTTP(wr http.ResponseWriter, r *http.Request) {
	p.log.Debug("ServeHTTP now.")

	conn, err := websocket.Accept(wr, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	})

	if err != nil {
		p.log.Error("Error: %v", err)
		return
	}
	defer conn.Close(websocket.StatusInternalError, "fail")

	ctx := r.Context()
	select {
	case <-p.stopChan:
		return
	default:
		p.connectionChan <- websocket.NetConn(ctx, conn, websocket.MessageBinary)
		select {
		case <-p.stopChan:
		case <-r.Context().Done():
		}
	}
	conn.Close(websocket.StatusNormalClosure, "ok")
}

func (p *WebSocketProxyServer) Accept() (net.Conn, error) {
	p.log.Debug("Accepting connection now.")

	select {
	case <-p.stopChan:
		return nil, fmt.Errorf("server stopped")
	case err := <-p.errorChan:
		_ = p.Close()
		return nil, err
	case c := <-p.connectionChan:
		return c, nil
	}
}

func (p *WebSocketProxyServer) Close() error {
	p.log.Debug("Closing now.")

	select {
	case <-p.stopChan:
	default:
		close(p.stopChan)
	}
	if p.server != nil {
		return p.server.Close()
	}

	return nil
}

type webSocketProxyAddr struct {
	address string
}

// Name of the network (for example, "tcp", "udp")
func (w *webSocketProxyAddr) Network() string {
	return "tcp"
}

// string form of address (for example, "192.0.2.1:25", "[2001:db8::1]:80")
func (w *webSocketProxyAddr) String() string {
	return w.address
}

func (p *WebSocketProxyServer) Addr() net.Addr {
	return &webSocketProxyAddr{
		address: p.address,
	}
}
