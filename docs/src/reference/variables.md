# Variables

Variables are part of a workflow instance and represents the data of the instance. A variable has a name and a JSON value. The visibility of a variable is defined by its variable scope.

## Variable Values

The value of a variable is stored as JSON object. It must have one of the following types:

* String
* Number
* Boolean
* Array
* Document/Object
* Null

## Access Variables

Variables can be accessed within the workflow instance, for example, in input/output mappings or conditions. In the expression the variable is accessed by its name. If the variable is a document then the nested properties can be accessed via dot notation.

For example:

Variable `order` has the value `{"id": "order-123", "totalPrice": 25.0}`.

The nested value can be accessed with the expression `order.id`.

## Variable Scopes

Variable scopes define the visibility of variables. The root scope is the workflow instance itself. Variables in this scope are visible everywhere in the workflow.

When the workflow instance enters a sub-process or an activity then a new scope is created. Activities in this scope can see all variables of this and of higher scopes (e.g. the root scope). But activities of higher scopes can not see variables of lower scopes.  

> TODO: image

The scope of a variable is defined when the variable is created. By default, variables are created in the root scope.

### Variable Propagation

When variables are merged into a workflow instance (e.g. on job completion, on message correlation) then each variable is propagated from the scope of the activity to its higher scopes.

The propagation ends when a scope contains a variable with the same name. In this case, the variable value is updated.

If no scope contains this variable then it is created as new variable in the root scope.

> TODO example

### Local Variables

In same cases, variables should be set in a given scope, even if they don't exist in this scope before.

In order to deactivate the variable propagation, the variables are set as *local variables*. That means that the variables are created or updated in the given scope, whether they exist in this scope before or not.

## Input/Output Variable Mapping

> TODO

Variable mappings can be used to transfer variables between scopes.

### Input Mappings

## Output Mappings

## Encoded as Message Pack

For performance reasons, the variable value is encoded using [MessagePack](https://msgpack.org/). MessagePack allows the broker to traverse a JSON document on the binary level without interpreting it as text and without need for complex parsing.

As a user, you do not need to deal with MessagePack. The client libraries take care of converting between MessagePack and JSON.
