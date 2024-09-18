package websocket

import (
	"context"
	"net/http"
	"time"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
	"golang.org/x/time/rate"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type WebsocketServer struct {
	port int

	log logger.Logger
}

func NewWebsocketServer(port int) *WebsocketServer {
	srv := &WebsocketServer{
		port: port,
	}
	config.InitLogger(&srv.log, srv)

	return srv
}

func (s *WebsocketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) error {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{})

	if err != nil {
		s.log.Error("Failed to accept websocket connection because: %v", err)
		return err
	}

	defer c.CloseNow()

	l := rate.NewLimiter(rate.Every(time.Millisecond*100), 10)
	for {
		err = s.handleMessage(r.Context(), c, l)

		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return nil
		}

		if err != nil {
			s.log.Error("Failed to echo with %v: %v", r.RemoteAddr, err)
			return err
		}
	}
}

func (s *WebsocketServer) handleMessage(ctx context.Context, c *websocket.Conn, l *rate.Limiter) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	err := l.Wait(ctx)
	if err != nil {
		return err
	}

	var msg map[string]interface{}
	err = wsjson.Read(ctx, c, &msg)
	if err != nil {
		s.log.Error("Failed to read message because: %v", err)
		return err
	}

	s.log.Debug("Received message: %v", msg)

	return nil
}
