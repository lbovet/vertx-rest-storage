vertx-rest-storage
==================

Persistence for REST resources in the filesystem or a redis database. 

Stores resources in a hierarchical way according to their URI.

It uses usual mime mapping to determine content type. Without extension, JSON is assumed.

The following methods are supported on leaves:
* GET: Returns the content of the resource.
* PUT: Stores the content of the resource.

The following methods are supported on intermediate nodes (collections):
* GET: Returns the list of collection members.
* POST: Stores the content under a new resources named with a random UUID. 

Dependencies
------------

Redis persistence uses the redis-client busmod "de.marx-labs.redis-client" 
