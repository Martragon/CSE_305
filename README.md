# Changes outside of the dao:
* In src/main/webapp/viewAddOrder.jsp, changed line 35 to name="orderPercentage"
* Added parameter private String date into the /src/main/java/model/Stock.java with getter and setter methods
* In /src/main/java/resources/EditCustomerController.java changed customerID to customerId in line 39
* In /src/main/java/resources/DeleteCustomerController.java changed customerID to customerId in line 38
* Added setters and getters into /src/main/java/model/Account.java
* In /src/main/java/resources/GetCustomersController.java removed searchKeyword from method dao.getCustomers() in line 45
* In src/main/webapp/ShowStocks.jsp added a new line after line 17, inserting <th>Price</th>, and line 27 (previously line 26) inserted <td>${cd.price}</td>
* Added new variable 'private String priceDate' to src/main/java/model/Stock.java with getter and setter methods