vertx-rest-storage
==================

Persistence for REST resources in the filesystem or a redis database. 

Stores resources in a hierarchical way according to their URI. It actually implements a generic CRUD REST service.

It uses usual mime mapping to determine content type so you can also use it as a web server. Without extension, JSON is assumed.

The following methods are supported on leaves (documents):
* GET: Returns the content of the resource.
* PUT: Stores the request body in the resource.
* DELETE: Deletes the resource.

The following methods are supported on intermediate nodes (collections):
* GET: Returns the list of collection members. Serves JSON and HTML representations.
* DELETE: Delete the collection and all its members.

Runs either as a module or can be integrated into an existing application by instantiating the RestStorageHandler class directly.

Configuration
-------------

    {
        "port": 8989        // Port we listen to. Defaults to 8989.
        "storage": ...,     // The type of storage (see below). Defaults to "filesystem".		                         
        "prefix": "/test",  // The part of the URL path before this handler (aka "context path" in JEE terminology). 
                            // Defaults to "/".
    }

### File System Storage

    {
        "storage": "filesystem",                         
        "root": "/test",    // The directory where the storage has its root (aka "document root" in Apache terminology).
                            // Defaults to "."
    }

### Redis Storage

	{
		"storage": "redis",      
		"address": "redis-client2" // The event bus address of the redis client busmod. 
		                           // Defaults to "redis-client".                  
		"root": "test",            // The prefix for the redis keys containing the data. 
		                           // Defaults to "rest-storage". 
		                        
	}

Dependencies
------------

Redis persistence uses the redis-client busmod "de.marx-labs.redis-client". You must deploy it yourself.
