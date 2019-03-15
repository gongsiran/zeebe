# JSON Payload Mapping

This section provides examples for mapping JSON payloads. In the context of workflow execution, there are three types of mappings:

1. [Input mappings](reference/json-payload-mapping.html#input-mapping) map workflow instance payload to task payload.
1. [Output mappings](reference/json-payload-mapping.html#output-mapping) map task payload back into workflow instance payload.
1. [Merging mappings](reference/json-payload-mapping.html#merging-mapping) on end events or sequence flows that lead to parallel gateways.

## Input Mapping

<table>
  <tr>
    <th>Description</th>
    <th>Workflow Instance Payload</th>
    <th>Input Mapping</th>
    <th>Task Payload</th>
  </tr>

  <tr>
    <td>
    Default
    </td>
    <td><pre>
{
 "price": 342.99,
 "productId": 41234
}
    </pre></td>
    <td><pre>
none
    </pre></td>
    <td><pre>
{
 "price": 342.99,
 "productId": 41234
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Copy entire payload
    </td>
    <td><pre>
{
 "price": 342.99,
 "productId": 41234
}
    </pre></td>
    <td><pre>
Source: $
Target: $
    </pre></td>
    <td><pre>
{
 "price": 342.99,
 "productId": 41234
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Move payload into new object
    </td>
    <td><pre>
{
 "price": 342.99,
 "productId": 41234
}
    </pre></td>
    <td><pre>
Source: $
Target: $.orderedItem
    </pre></td>
    <td><pre>
{
  "orderedItem": {
    "price": 342.99,
    "productId": 41234
  }
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Extract object
    </td>
    <td><pre>
{
 "address": {
    "street": "Borrowway 1",
    "postcode": "SO40 9DA",
    "city": "Southampton",
    "country": "UK"
  },
 "name": "Hans Horn"
}
    </pre></td>
    <td><pre>
Source: $.address
Target: $
    </pre></td>
    <td><pre>
{
  "street": "Borrowway 1",
  "postcode": "SO40 9DA",
  "city": "Southampton",
  "country": "UK"
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Extract and put into new object
    </td>
    <td><pre>
{
 "address": {
    "street": "Borrowway 1",
    "postcode": "SO40 9DA",
    "city": "Southampton",
    "country": "UK"
  },
 "name": "Hans Horn"
}
    </pre></td>
    <td><pre>
Source: $.address
Target: $.newAddress
    </pre></td>
    <td><pre>
{
 "newAddress":{
  "street": "Borrowway",
  "postcode": "SO40 9DA",
  "city": "Southampton",
  "country": "UK"
 }
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Extract and put into new objects
    </td>
    <td><pre>
{
 "order":
 {
  "customer:{
   "name": "Hans Horst",
   "customerId": 231
  },
  "price": 34.99
 }
}
    </pre></td>
    <td><pre>
Source: $.order.customer
Target: $.new.details
    </pre></td>
    <td><pre>
{
 "new":{
   "details": {
     "name": "Hans Horst",
     "customerId": 231
  }
 }
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Extract array and put into new array
    </td>
    <td><pre>
{
 "name": "Hans Hols",
 "numbers": [
   "221-3231-31",
   "312-312313",
   "31-21313-1313"
  ],
  "age": 43
{
    </pre></td>
    <td><pre>
Source: $.numbers
Target: $.contactNrs
    </pre></td>
    <td><pre>
{
 "contactNrs": [
   "221-3231-31",
   "312-312313",
   "31-21313-1313"
  ]
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Extract single array value and put into new array
    </td>
    <td><pre>
{
 "name": "Hans Hols",
 "numbers": [
   "221-3231-31",
   "312-312313",
   "31-21313-1313"
  ],
  "age": 43
{
    </pre></td>
    <td><pre>
Source: $.numbers[1]
Target: $.contactNrs[0]
    </pre></td>
    <td><pre>
{
 "contactNrs": [
   "312-312313"
  ]
 }
}
    </pre></td>
  </tr>

  <tr>
    <td>
    Extract single array value and put into property
    </td>
    <td><pre>
{
 "name": "Hans Hols",
 "numbers": [
   "221-3231-31",
   "312-312313",
   "31-21313-1313"
  ],
  "age": 43
{
    </pre></td>
    <td><pre>
Source: $.numbers[1]
Target: $.contactNr
    </pre></td>
    <td><pre>
{
 "contactNr": "312-312313"
 }
}
    </pre></td>
  </tr>

</table>

## Output Mapping


All examples assume *merge* output behavior.

<table>

  <tr>
    <th>Description</th>
    <th>Job Payload</th>
    <th>Workflow Instance Payload</th>
    <th>Output Mapping</th>
    <th>Result</th>
  </tr>
<!-- NEW ROW -->
  <tr>
  <td>Default Merge</td>
  <td><pre>
{
 "sum": 234.97
}
  </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99]
}
  </pre></td>

  <td><pre> none </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99],
 "sum": 234.97
}
  </pre></td>
  </tr>

<!-- NEW ROW -->
  <tr>
  <td>Default Merge without workflow payload</td>
  <td><pre>
{
 "sum": 234.97
}
  </pre></td>

  <td><pre>
{
}
  </pre></td>

  <td><pre> none </pre></td>

  <td><pre>
{
 "sum": 234.97
}
  </pre></td>
  </tr>

<!-- NEW ROW -->
  <tr>
  <td>Replace with entire payload</td>
  <td><pre>
{
 "sum": 234.97
}
  </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99]
}
  </pre></td>

  <td><pre>
Source: $
Target: $
  </pre></td>

  <td><pre>
{
 "sum": 234.97
}
  </pre></td>
  </tr>

<!-- NEW ROW -->
  <tr>
  <td>Merge payload and write into new object</td>
  <td><pre>
{
 "sum": 234.97
}
  </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99]
}
  </pre></td>

  <td><pre>
Source: $
Target: $.total
  </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99],
 "total": {
  "sum": 234.97
 }
}
  </pre></td>
  </tr>

<!-- NEW ROW -->
<tr>
  <td>Replace payload with object value</td>
  <td><pre>
{
 "order":{
  "id": 12,
  "sum": 21.23
 }
}
  </pre></td>

  <td><pre>
{
 "ordering": true
}
  </pre></td>

  <td><pre>
Source: $.order
Target: $
  </pre></td>

  <td><pre>
{
  "id": 12,
  "sum": 21.23
}
  </pre></td>
</tr>


<!-- NEW ROW -->
<tr>
  <td>Merge payload and write into new property</td>
  <td><pre>
{
 "sum": 234.97
}
  </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99]
}
  </pre></td>

  <td><pre>
Source: $.sum
Target: $.total
  </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99],
 "total": 234.97
}
  </pre></td>
</tr>


<!-- NEW ROW -->

<tr>
  <td>Merge payload and write into array</td>
  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99]
}
  </pre></td>

  <td><pre>
{
 "orderId": 12
}
  </pre></td>

  <td><pre>
Source: $.prices
Target: $.prices
  </pre></td>

  <td><pre>
{
 "orderId": 12,
 "prices": [
   199.99,
   29.99,
   4.99]
}
  </pre></td>
</tr>


<tr>
  <td>Merge and update array value</td>
  <td><pre>
{
 "newPrices": [
   199.99,
   99.99,
   4.99]
}
  </pre></td>

  <td><pre>
{
 "prices": [
   199.99,
   29.99,
   4.99]
}
  </pre></td>

  <td><pre>
Source: $.newPrices[1]
Target: $.prices[0]
  </pre></td>

  <td><pre>
{
 "prices": [
   99.99,
   29.99,
   4.99]
}
  </pre></td>
</tr>

<tr>
  <td>Extract array value and write into payload</td>
  <td><pre>
{
 "newPrices": [
   199.99,
   99.99,
   4.99]
}
  </pre></td>

  <td><pre>
{
 "orderId": 12
}
  </pre></td>

  <td><pre>
Source: $.newPrices[1]
Target: $.price
  </pre></td>

  <td><pre>
{
 "orderId": 12,
 "price": 99.99
}
  </pre></td>
</tr>

</table>
