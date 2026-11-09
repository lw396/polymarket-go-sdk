# PolyBolt crypto prices

The opt-in `pkg/polybolt` package implements authenticated
`price.crypto` and `price.crypto.twap` (60-second) subscriptions.

It does **not** change the root SDK client's constructors, start extra sockets,
replace RTDS, place orders, or derive credentials. Existing RTDS activity streams
remain separate.

Protocol references:
- [Migration guide](https://docs.polymarket.com/migrate/rtds-to-polybolt)
- [Wire protocol](https://docs.polymarket.com/api-reference/wss/polybolt)

## Connect

```go
cfg := polybolt.DefaultConfig()
cfg.Credentials = auth.APIKey{
    Key:        os.Getenv("POLY_API_KEY"),
    Secret:     os.Getenv("POLY_API_SECRET"),
    Passphrase: os.Getenv("POLY_API_PASSPHRASE"),
}
cfg.Subscriptions = []polybolt.Subscription{
    {
        Channel: polybolt.CryptoPrices,
        Filter:  polybolt.Filter{Symbol: "btcusd", Provider: "chainlink"},
    },
    {
        Channel: polybolt.CryptoTWAP,
        Filter:  polybolt.Filter{Symbol: "btcusd", WindowSeconds: 60},
    },
}

client, err := polybolt.Dial(ctx, cfg)
if err != nil {
    return err
}
defer client.Close()

for event := range client.Events() {
    if event.Snapshot {
        for _, point := range event.Payload.Data {
            // Initialize history with point.Timestamp and point.FullAccuracyValue.
        }
        continue
    }
    price, err := event.Payload.PricePoint.Decimal()
    if err != nil {
        return err
    }
    // Use price and event.Payload.Timestamp (Unix milliseconds).
    _ = price
}
return client.Err()
```

See [the runnable example](../examples/polybolt/main.go).
Use existing **CLOB L2 credentials**, not a wallet private key. Never log the
configuration, auth frames, or credentials. A custom `websocket.Dialer` can be
supplied through `Config.Dialer` for proxy/TLS settings.

## Contract

- Each client owns **one connection and a fixed subscription set**. To change
  subscriptions, close it and dial a replacement. There is no connection
  sharding or dynamic subscribe/unsubscribe in this version.
- Supply 1–64 distinct subscriptions. Crypto symbols must be explicit lowercase
  USD pairs, e.g. `btcusd`, not `btc/usd`, `BTCUSD`, or `btcusdt`.
  Spot filters cannot specify a window; TWAP requires `WindowSeconds: 60`.
- `Dial` waits for authentication and **every** subscription acknowledgement.
  Partial acceptance closes the socket and returns an error. Initial failures
  are returned directly rather than retried. Early snapshots are buffered until
  the handshake completes.
- The context passed to `Dial` owns the **entire client lifetime**, not just the
  handshake. Canceling it is a clean shutdown. For a handshake deadline, set
  `Config.HandshakeTimeout` instead of canceling a short-lived dial context.
- One consumer reads `Events()`; snapshots and live points remain in wire order.
  A snapshot's `Data` can be empty and contains only the server's recent history
  (currently about two minutes), not a guarantee of any particular timestamp.
- `FullAccuracyValue` is an **exact decimal price string**, not an E18 integer.
  `PricePoint.Decimal()` parses it without float conversion. The SDK never
  divides prices by 1e18 or substitutes the float `Value` for a missing exact
  value. No stale-price interpolation or application-specific rounding is done.
- `Payload.Source` exposes the actual provider. A filter's `Provider` is only a
  request: the server may ignore it or fall back to its owner. Consumers that
  depend on Chainlink must verify the source; a `btcusd` label alone does not
  guarantee the provider or convert a Binance USDT quote into USD.
- `Seq` is scoped to **ConnectionID + Channel**, not to a symbol. It resets on
  reconnect and includes snapshot frames. `ConnectionID` increments after
  successful recovery. `Gap` exposes skipped sequence numbers; `Dropped`
  preserves the server's slow-consumer counter. They can overlap: do not sum them.
  Consumers decide whether to invalidate state after a loss.
- A full local buffer terminates the client with `ErrSlowConsumer` rather than
  silently losing data. Already-buffered events can still be drained. Inspect
  `Err()` after the event channel closes.
- `Errors()` provides interruption notifications, including whether recovery is
  planned. It retains **only the latest unread notification**; `Err()` always
  preserves the terminal failure. Reading notifications is optional.
- `Close()` is idempotent, cancels backoff/dialing, closes the socket and waits
  for the reader. `Done()` closes after both output channels close. Context
  cancellation and explicit Close leave `Err()` nil.

## Recovery and limits

Start from `DefaultConfig()` to enable automatic recovery. The zero-value
`Reconnect` flag is false. Defaults are a 30-second handshake timeout, 75-second
read timeout, 256 buffered events, two-second reconnect delay, and five
consecutive failed recovery handshakes. Set `MaxReconnectAttempts: -1` for
unlimited attempts; zero selects the default. A successful recovery handshake
resets the attempt budget.

Recovery reauthenticates, restores all subscriptions and delivers fresh snapshots.
It does not promise gapless history. The client responds to server WebSocket
pings with pongs and does not send legacy RTDS text `PING` messages.

- Close `4003`: reconnect after uniform jitter in [0, 10 seconds).
- Close `4001`, `4008`, or `1008`: terminal; no blind retry.
- Rejections are typed `*RejectedError` with a machine-readable `Code`;
  only `auth_unavailable` is considered transient during recovery.
- Network failures, read timeouts and close `4002` are recoverable when enabled.
- Malformed frames, unexpected acknowledgements, unsupported envelope versions,
  non-increasing sequences and oversized messages are terminal protocol errors.
- Outgoing auth/subscription frames are capped at 64 KiB. Subscriptions use one
  batch per connection, avoiding the server's 20-subscription-frame/second limit.

Server free-text error/close messages and raw authentication frames are not
included in SDK errors.

## Testing

```sh
go test -race ./pkg/polybolt
```

Tests use local WebSocket servers and fake credentials only. Production
authentication and agreement with settlement prices still require a separate
operator-run verification before trading.
