# vertx-rest-storage

[![Build Status](https://drone.io/github.com/swisspush/vertx-rest-storage/status.png)](https://drone.io/github.com/swisspush/vertx-rest-storage/latest)
[![Maven Central](https://img.shields.io/maven-central/v/org.swisspush/rest-storage.svg)]()

Persistence for REST resources in the filesystem or a redis database. 

Stores resources in a hierarchical way according to their URI. It actually implements a generic CRUD REST service.

It uses usual mime mapping to determine content type so you can also use it as a web server. Without extension, JSON is assumed.

The following methods are supported on leaves (documents):
* GET: Returns the content of the resource.
* PUT: Stores the request body in the resource.
* DELETE: Deletes the resource.

The following methods are supported on intermediate nodes (collections):
* GET: Returns the list of collection members. Serves JSON and HTML representations.
* POST (StorageExpand): Returns the expanded content of the sub resources of the (collection) resource. The depth is limited to 1 level. See description below
* DELETE: Delete the collection and all its members.

Runs either as a module or can be integrated into an existing application by instantiating the RestStorageHandler class directly.

## Features
### GET
Invoking GET request on a leave (document) returns the content of the resource.
> GET /storage/resources/resource_1

Invoking GET request on a collection returns a list of collection members.
> GET /storage/resources/

#### Parameters

| Parameter | Description  |
|:--------- | :----------- |
| limit | defines the amount of returned resources |
| offset | defines the amount of resources to skip. Can be used in combination with limit to provide pageing functionality |

##### Examples
Given a collection of ten items (res1-res10) under the path /server/tests/offset/resources/

| Request | Returned items  |
|:--------- | :----------- |
| **GET** /server/tests/offset/resources/?limit=10 | all |
| **GET** /server/tests/offset/resources/?limit=99 | all |
| **GET** /server/tests/offset/resources/?limit=5 | res1,res10,res2,res3,res4 |
| **GET** /server/tests/offset/resources/?offset=2 | res2,res3,res4,res5,res6,res7,res8,res9 |
| **GET** /server/tests/offset/resources/?offset=11 | no items (empty array) |
| **GET** /server/tests/offset/resources/?offset=2&limit=-1 | res2,res3,res4,res5,res6,res7,res8,res9 |
| **GET** /server/tests/offset/resources/?offset=0&limit=3 | res1,res10,res2 |
| **GET** /server/tests/offset/resources/?offset=1&limit=10 | res10,res2,res3,res4,res5,res6,res7,res8,res9 |

The returned json response look like this:

```json
{
  "resources": [
    "res1",
    "res10",
    "res2",
    "res3",
    "res4"
  ]
}
```

### StorageExpand

The StorageExpand feature expands the hierarchical resources and returns them as a single concatenated json resource.

Having the following resources in the storage

```sh
key: data:test:collection:resource1     value: {"myProp1": "myVal1"}
key: data:test:collection:resource2     value: {"myProp2": "myVal2"}
key: data:test:collection:resource3     value: {"myProp3": "myVal3"}
```
would lead to this result

```json
{
    "collection" : {
        "resource1" : {
            "myProp1": "myVal1"
        },
        "resource2" : {
            "myProp2": "myVal2"
        },
        "resource3" : {
            "myProp3": "myVal3"
        }
    }
}
```
    
##### Usage

To use the StorageExpand feature you have to make a POST request to the desired collection to expand having the url paramter **storageExpand=true**. Also you wil have
to send the names of the subresources in the body of the request. Using the example above, the request would look like this:

**POST /yourStorageURL/collection** with the body:
```json
{
    "subResources" : ["resource1", "resource2", "resource3"]
}
```
### Lock Mechanism
The lock mechanism allows you to lock a resource for a specified time. This way only the owner of the lock is able to write or delete the given resource.
To lock a resource, you have to add the following headers to your PUT / DELETE request.

| Headers | Type | Default value | Description |
|:------- | :--- | :------------ | :---------- |
|x-lock | String | | The owner of the lock. |
|x-lock-mode | silent | **silent** | Any PUT or DELETE performed on this resource without the valid owner will have no effect and get *200 OK* back.  |
|             | reject |  | Any PUT or DELETE performed on this resource without the valid owner will have no effect and get *409 Conflict* back. |
|x-lock-expire-after | long | **300** | Defines the lock lifetime. The default value is set to *300* seconds.  |

`Warning:` The lock will be always removed if you perform a DELETE on a collection containing a locked resource. There is no check for locks in collections.

## Configuration

The following configuration values are available:

| Property | Type | Default value | Description | 
|:--------- | :----------- | :----------- | :----------- |
| root | common | . | The prefix for the directory or redis key |
| storageType | common | filesystem | The storage implementation to use. Choose between filesystem or redis |
| port | common | 8989 | The port the mod listens to. |
| prefix | common | / | The part of the URL path before this handler (aka "context path" in JEE terminology) |
| storageAddress | common | resource-storage | The eventbus address the mod listens to. |
| editorConfig | common |  | Additional configuration values for the editor |
| redisHost | redis | localhost | The host where redis is running on |
| redisPort | redis | 6379 | The port where redis is running on |
| expirablePrefix | redis | rest-storage:expirable | The prefix for expirable data redis keys |
| resourcesPrefix | redis | rest-storage:resources | The prefix for resources redis keys |
| collectionsPrefix | redis | rest-storage:collections | The prefix for collections redis keys |
| deltaResourcesPrefix | redis | delta:resources | The prefix for delta resources redis keys |
| deltaEtagsPrefix | redis | delta:etags | The prefix for delta etags redis keys |
| lockPrefix | redis | rest-storage:locks | The prefix for lock redis keys |
| resourceCleanupAmount | redis | 100000 | The maximum amount of resources to clean in a single cleanup run |

### Configuration util

The configurations have to be passed as JsonObject to the module. For a simplyfied configuration the _ModuleConfigurationBuilder_ can be used.

Example:

```java
ModuleConfiguration config = with()
		.redisHost("anotherhost")
		.redisPort(1234)
		.editorConfig(new JsonObject().put("myKey", "myValue"))
		.build();

JsonObject json = config.asJsonObject();
```

Properties not overriden will not be changed. Thus remaining default.

To use default values only, the _ModuleConfiguration_ constructor without parameters can be used:

```java
JsonObject json  = new ModuleConfiguration().asJsonObject();
```

## Storage types
Currently there are two storage types supported. File system storage and redis storage.
### File System Storage
The data is stored hierarchically on the file system. This is the default storage type when not overriden in the configuration.

### Redis Storage
The data is stored in a redis database.
Caution: The redis storage implementation does not currently support streaming. Avoid transfering too big payloads since they will be entirely copied in memory.

## Dependencies
This module uses Vert.x v3.2.0 (or later), so **Java 8** is required.

## Use gradle with alternative repositories
As standard the default maven repositories are set.
You can overwrite these repositories by setting these properties (`-Pproperty=value`):

* `repository` this is the repository where resources are fetched
* `uploadRepository` the repository used in `uploadArchives` for releases
* `snapshotRepository` the repository used in `uploadArchives` for snapshot
* `repoUsername` the username for uploading archives
* `repoPassword` the password for uploading archives


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/lbovet/vertx-rest-storage/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
