MQTT -> NanoMQ?
----

https://www.emqx.com/en/blog/open-mqtt-benchmarking-comparison-mosquitto-vs-nanomq#strong-fan-out-strong-fanout-5-1000-5-250k
> Fan-out: fanout-5-1000-5-250K
>    5 publishers, 5 topics, 1000 subscribers (each sub to all topics)
>    Publish rate: 250/s, so sub rate = 250*1000 = 250k/s
>    QoS 1, payload 16B
> For example, in the fan-out scenario, Mosquitto can only support an outgoing message rate of around 82k per second, while NanoMQ can keep a stable rate of 250k. Even NanoMQ can stably handle message throughput of up to 500k on this c5.4xlarge virtual machine.
> Average pub-to-sub latency (ms) 13.91


https://docs.docker.com/reference/compose-file/deploy/

  image: emqx/nanomq:latest-slim  # https://hub.docker.com/r/emqx/nanomq
  deploy:
    replicas: 6
    endpoint_mode: dnsrr
  etc/nanomq.conf
  etc/nanomq_acl.conf
https://www.npmjs.com/package/u8-mqtt
https://nanomq.io/docs/en/latest/config-description/acl.html

https://chatgpt.com/c/68cac7bb-7c7c-832b-8709-cc6738ebfc51
`nanomq.conf`
```
system {
  allow_anonymous = true
}

listeners {
  tcp {
    bind = "0.0.0.0:1883"
  }
  ws {
    bind = "0.0.0.0:9001"
  }
}

auth {
  allow_anonymous = true
  no_match = allow
  deny_action = disconnect
  # optionally define user “write_user” if we want TCP clients to use that
  # password = { include "nanomq_pwd.conf" }
  acl = { include "nanomq_acl.conf" }
}

# NanoMQ supports retained messages that survive broker restarts, using a SQLite-backed persistence mechanism.
# https://nanomq.io/docs/en/latest/tutorial/retain-msg-persistence.html
sqlite {
  disk_cache_size = 102400
  mounted_file_path = "/var/lib/nanomq/sqlite"
  flush_mem_threshold = 10
  resend_interval = 5000
}
```
`nanomq_acl.conf`
```
rules = [
  # Anonymous: read-only
  {"permit": "allow", "username": "#", "action": "subscribe", "topics": ["#"]},
  {"permit": "deny", "username": "#", "action": "publish", "topics": ["#"]},

  # KnownUser/Password: full read-write
  {"permit": "allow", "username": "write_user", "action": "pubsub", "topics": ["#"]}
]
```
