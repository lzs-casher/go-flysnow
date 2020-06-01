package flysnow

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"
)

var errmap = map[int]error{
	1002: errors.New("tag error"),
	1003: errors.New("sys time out"),
	1001: errors.New("op error"),
	0:    nil,
}
var TimeOut time.Duration

func transerr(code int) error {
	if v, ok := errmap[code]; ok {
		return v
	}
	return errors.New("sys error")
}

const (
	ConstHeader = "^"
	EndId       = "$"
	CodeLength  = 4
	TagLength   = 4
	DataLength  = 4
)

type Resp struct {
	Code int
	Data []byte
}
type FlySnowConn struct {
	Addr string
	Port int
	conn net.Conn
	b    []byte

	p *FlySnowPool
	t time.Time
}
type StatQuery struct {
	Term         string
	DataQuery    map[string]interface{}
	Index        map[string]interface{}
	STime, ETime int64
	Group        []string
	Limit, Skip  int
	Sort         []interface{}
	Span         int64
	Spand        string
}
type Clear struct {
	TagTerms map[string][]string `json:"tag_terms"`
	Query    bson.M              `json:"query"`
	STime    int64               `json:"s_time"`
	ETime    int64               `json:"e_time"`
}

type Data struct {
	Op   int
	Tag  string
	Body []byte
}

func NewStatQuery() *StatQuery {
	return &StatQuery{}
}
func (sq *StatQuery) SetSort(key string, asc bool) {
	sq.Sort = []interface{}{key, asc}
}

//d=[s,h,d,m,y]
func (sq *StatQuery) SetSpan(t int64, d string) {
	switch d {
	case "s", "d", "h", "m", "y":
		sq.Spand = d
		sq.Span = t
	default:
	}
}

//链接错误重连
func (f *FlySnowConn) Reconnection() (err error) {
	f.conn, err = createfsconn(f.Addr, f.Port)
	return err
}

//不需要返回的发送
func (f *FlySnowConn) SendWithOutResp(tag string, data interface{}) error {
	_, err := f.sender(data, 2, 0, tag)
	return err
}

//正常发送
func (f *FlySnowConn) Send(tag string, data interface{}) (result *Resp, err error) {
	_, err = f.sender(data, 2, 1, tag)
	if err != nil {
		return nil, err
	}
	return f.Reader()
}

//Ping
func (f *FlySnowConn) Ping() (err error) {
	_, err = f.sender(nil, 0, 1, "")
	if err != nil {
		return err
	}
	_, err = f.Reader()
	return err
}

//统计查询
func (f *FlySnowConn) Stat(tag string, query *StatQuery) (result *Resp, err error) {
	_, err = f.sender(query, 1, 1, tag)
	if err != nil {
		return nil, err
	}
	return f.Reader()
}

//统计清理
func (f *FlySnowConn) Clear(tag string, clear *Clear) (result *Resp, err error) {
	_, err = f.sender(clear, 3, 1, "")
	if err != nil {
		return nil, err
	}
	return f.Reader()
}

// 手动调用归档，当有自动归档进行时，返回错误
func (f *FlySnowConn) Rotate() (result *Resp, err error) {
	_, err = f.sender("", 4, 1, "")
	if err != nil {
		return nil, err
	}
	return f.Reader()
}

//读取返回数据
func (f *FlySnowConn) Reader() (result *Resp, err error) {
	tmpBuffer := make([]byte, 0)
	if TimeOut != 0 {
		f.conn.SetReadDeadline(time.Now().Add(TimeOut))
	}
	for {
		i, err := f.conn.Read(f.b)
		if err != nil {
			return nil, err
		}
		result, tmpBuffer = f.UnPacket(append(tmpBuffer, f.b[:i]...))
		if result != nil && len(tmpBuffer) == 0 {
			return result, nil
		}
	}
}

func (f *FlySnowConn) sender(data interface{}, op, resp int, tag string) (int, error) {
	if TimeOut != 0 {
		f.conn.SetWriteDeadline(time.Now().Add(TimeOut))
	}
	return f.conn.Write(f.Packet(JsonEncode(data), op, resp, tag))
}
func JsonEncode(data interface{}) []byte {
	b, _ := json.Marshal(data)
	return b
}

//数据包长度 = []byte(ConstHeader)+[]byte(op)+Len(tag)+[]byte(tag)+BodyDataLength+[]byte(body)+[]byte(resp)
//封包
func (f *FlySnowConn) Packet(message []byte, op, resp int, tag string) []byte {
	result := []byte(ConstHeader)
	result = append(result, IntToBytes(op)...)
	result = append(result, IntToBytes(len([]byte(tag)))...)
	result = append(result, []byte(tag)...)
	result = append(result, IntToBytes(len(message))...)
	result = append(result, message...)
	result = append(result, IntToBytes(resp)...)
	return result
}
func (f *FlySnowConn) UnPacket(body []byte) (*Resp, []byte) {
	l := len(body)
	for i := 0; i < l; i++ {
		if string(body[:i]) == EndId {
			code := BytesToInt(body[i : i+CodeLength])
			i = i + CodeLength
			datalen := BytesToInt(body[i : i+DataLength])
			i = i + DataLength
			if l < i+datalen {
				return nil, body
			} else {
				return &Resp{Code: code, Data: body[i : i+datalen]}, body[i+datalen:]
			}
		}
	}
	return nil, body
}

func (f *FlySnowConn) Close() error {
	f.p.put(f)
	return nil
}

func NewConnection(addr string, port int) (*FlySnowConn, error) {
	conn, err := createfsconn(addr, port)
	if err != nil {
		return nil, err
	}
	return &FlySnowConn{
		Addr: addr,
		Port: port,
		conn: conn,
		b:    make([]byte, 1024),
	}, nil
}

func createfsconn(addr string, port int) (net.Conn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr+fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	return net.DialTCP("tcp", nil, tcpAddr)
}

//整形转换成字节
func IntToBytes(n int) []byte {
	x := int32(n)

	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, x)
	return bytesBuffer.Bytes()
}

//字节转换成整形
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)

	var x int32
	binary.Read(bytesBuffer, binary.BigEndian, &x)
	return int(x)
}

// Pool maintains a pool of connections. The application calls the Get method
// to get a connection from the pool and the connection's Close method to
// return the connection's resources to the pool.
type FlySnowPool struct {
	Dial func() (*FlySnowConn, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(c *FlySnowConn) error

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout time.Duration

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// mu protects fields defined below.
	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	active int

	// Stack of idleConn with most recently used at the front.
	idle list.List
}

// Get gets a connection. The application must close the returned connection.
// This method always returns a valid connection so that applications can defer
// error handling to the first use of the connection. If there is an error
// getting an underlying connection, then the connection Err, Do, Send, Flush
// and Receive methods return that error.
func (p *FlySnowPool) Get() (*FlySnowConn, error) {
	c, err := p.get()
	if err != nil {
		return nil, err
	}
	c.p = p
	return c, nil
}

// ActiveCount returns the number of active connections in the pool.
func (p *FlySnowPool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// Close releases the resources used by the pool.
func (p *FlySnowPool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(FlySnowConn).conn.Close()
	}
	return nil
}

// release decrements the active count and signals waiters. The caller must
// hold p.mu during the call.
func (p *FlySnowPool) release() {
	p.active--
	if p.cond != nil {
		p.cond.Signal()
	}
}

// get prunes stale connections and returns a connection from the idle list or
// creates a new connection.
func (p *FlySnowPool) get() (*FlySnowConn, error) {
	p.mu.Lock()

	// Prune stale connections.
	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(*FlySnowConn)
			if ic.t.Add(timeout).After(time.Now()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.conn.Close()
			p.mu.Lock()
		}
	}

	for {

		// Get idle connection.
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(*FlySnowConn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()
			if test == nil || test(ic) == nil {
				return ic, nil
			}
			ic.conn.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.
		if p.closed {
			p.mu.Unlock()
			return nil, errors.New("flysnow: get on closed pool")
		}

		// Dial new connection if under limit.
		if p.MaxActive == 0 || p.active < p.MaxActive {
			p.active++
			p.mu.Unlock()
			c, err := p.Dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}

		if !p.Wait {
			p.mu.Unlock()
			return nil, errors.New("flysnow: connection pool exhausted")
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

func (p *FlySnowPool) put(c *FlySnowConn) error {
	p.mu.Lock()
	if !p.closed {
		c.t = time.Now()
		p.idle.PushFront(c)
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(*FlySnowConn)
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()
	return c.conn.Close()
}

//获取queue的数据包
func QueueData(tag string, data interface{}) interface{} {
	return &Data{2, tag, JsonEncode(data)}
}
