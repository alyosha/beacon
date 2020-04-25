## beacon

**disclaimer:** this implementation is just for fun

Was thinking about publishing to PubSub from a number of IOT devices around my
home, and thought it would be nice to have a PubSub worker that could handle 
distinct events of any kind coming into a single topic. 

Also wanted to allow for both streaming / pull-on-schedule style workers, so 
used pubsub apiv1 to implement both types. 

See [examples](examples/main.go) for a working example.


