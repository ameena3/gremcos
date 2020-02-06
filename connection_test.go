package gremtune

import "testing"

func TestPanicOnMissingAuthCredentials(t *testing.T) {
	c := newClient()
	ws := &Websocket{}
	c.conn = ws

	defer func() {
		if r := recover(); r == nil {
			t.Fail()
		}
	}()

	c.conn.getAuth()
}
