# Variables

Variables are part of a workflow instance and represents the data the instance. A variable has a name and a JSON value. The visibility of a variable is defined by its variable scope.

## Variable Values

The value of a variable is stored as JSON object. It must have one of the following types:

* String
* Number
* Boolean
* Array
* Document/Object
* Null

## Access Variables

Variables can be accessed within the workflow instance, for example, in input/output mappings or conditions. In the expression, the variable is accessed by its name. If the variable is a document then the nested properties can be accessed via dot notation.

For example:

Variable `order` has the value `{"id": "order-123", "totalPrice": 25.0}`.

The nested value can be accessed with the expression `order.id`.

## Variable Scopes

> TODO

## Variable Propagation

> TODO

## Input/Output Variable Mapping

> TODO

Variable mappings can be used to transfer variables between scopes.

## Encoded as Message Pack

For performance reasons, the variable value is encoded using [MessagePack](https://msgpack.org/). MessagePack allows the broker to traverse a JSON document on the binary level without interpreting it as text and without need for complex parsing.

As a user, you do not need to deal with MessagePack. The client libraries take care of converting between MessagePack and JSON.
