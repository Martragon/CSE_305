package dao;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;

import model.Account;
import model.Customer;
import model.Location;

import java.util.stream.IntStream;

public class CustomerDao {
	/*
	 * This class handles all the database operations related to the customer table
	 */
    
    public Customer getDummyCustomer() {
        Location location = new Location();
        location.setZipCode(11790);
        location.setCity("Stony Brook");
        location.setState("NY");

        Account account = new Account();
        account.setAccountId(1);
        try {
            account.setCreationDate((new SimpleDateFormat("yyyy-MM-dd").parse("2020-10-10")));
        } catch (Exception e) {
        	e.printStackTrace();
        }
        
        Customer customer = new Customer();
        customer.setFirstName("Shiyong");
        customer.setId("111111111");
        customer.setLastName("Lu");
        customer.setEmail("shiyong@cs.sunysb.edu");
        customer.setSsn("111111111");
        customer.setAddress("321 Success Street");
        customer.setLocation(location);
        customer.setTelephone("5166328959");
        
        customer.setClientId("111111111");
        customer.setCreditCard("1234567812345678");
        customer.setRating(1);
        customer.setAccountCreationTime("2020-10-10");
        customer.setAccountNumber(1);
        
        return customer;
    }
    public List<Customer> getDummyCustomerList() {
        /*Sample data begins*/
        List<Customer> customers = new ArrayList<Customer>();

        for (int i = 0; i < 10; i++) {
            customers.add(getDummyCustomer());
        }
		/*Sample data ends*/

        return customers;
    }


    /**
	 * @param String searchKeyword
	 * @return ArrayList<Customer> object
	 */
    public List<Customer> getCustomers() {
        List<Customer> customers = new ArrayList<>();

        String sql = "SELECT p.ssn, p.firstname, p.lastname, p.email, " +
                     "p.address, p.city, p.state, p.zipcode, p.telephone, " +
                     "c.customerid, c.rating, c.cardnumber, " +
                     "a.accountcreated, a.accountid " +
                     "FROM customer c " +
                     "JOIN person p ON c.customerid = p.ssn " +
                     "JOIN account a ON c.customerid = a.customerid";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Customer customer = new Customer();
                Location location = new Location();

                // Set person info
                customer.setId(rs.getString("ssn"));
                customer.setSsn(rs.getString("ssn"));
                customer.setFirstName(rs.getString("firstname"));
                customer.setLastName(rs.getString("lastname"));
                customer.setEmail(rs.getString("email"));
                customer.setAddress(rs.getString("address"));
                customer.setTelephone(rs.getString("telephone"));

                // Set customer info
                customer.setClientId(rs.getString("customerid"));
                customer.setCreditCard(rs.getString("cardnumber"));
                customer.setRating(rs.getInt("rating"));

                // Set account info
                customer.setAccountCreationTime(rs.getString("accountcreated"));
                customer.setAccountNumber(rs.getInt("accountid"));

                // Set location info
                location.setCity(rs.getString("city"));
                location.setState(rs.getString("state"));
                location.setZipCode(rs.getInt("zipcode"));
                customer.setLocation(location);

                customers.add(customer);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return customers;
    }

	/**
	 * TODO See if it works
	 * 
	 * @param searchKeyword
	 * @return
	 */
    public List<Customer> getCustomers(String searchKeyword) {
        List<Customer> customers = new ArrayList<>();

        String sql = "SELECT p.ssn, p.firstname, p.lastname, p.email, " +
                     "p.address, p.city, p.state, p.zipcode, p.telephone, " +
                     "c.customerid, c.rating, c.cardnumber " +
                     "FROM customer c " +
                     "JOIN person p ON c.customerid = p.ssn " +
                     "WHERE p.email LIKE ?"; 

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, "%" + searchKeyword + "%");

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Customer customer = new Customer();
                    Location location = new Location();

                    customer.setClientId(rs.getString("customerid"));
                    customer.setSsn(rs.getString("ssn"));
                    customer.setFirstName(rs.getString("firstname"));
                    customer.setLastName(rs.getString("lastname"));
                    customer.setEmail(rs.getString("email"));
                    customer.setAddress(rs.getString("address"));
                    customer.setTelephone(rs.getString("telephone"));
                    customer.setCreditCard(rs.getString("cardnumber"));
                    customer.setRating(rs.getInt("rating"));

                    location.setCity(rs.getString("city"));
                    location.setState(rs.getString("state"));
                    location.setZipCode(rs.getInt("zipcode"));

                    customer.setLocation(location);
                    customers.add(customer);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return customers;
    }


	/*
	 * TODO See if it works
	 * This method fetches the customer who generated the highest total revenue and returns it
	 * The students code to fetch data from the database will be written here
	 * The customer record is required to be encapsulated as a "Customer" class object
	 */
	public Customer getHighestRevenueCustomer() {
	    String sql = "SELECT p.SSN, p.FirstName, p.LastName, p.Email, p.Address, p.City, p.State, p.ZipCode, p.Telephone, c.Rating, c.CardNumber, SUM(tx.Fee) AS TotalFeesPaid "
	    		+ "FROM Customer c JOIN Person p ON c.CustomerID = p.SSN "
	    		+ "JOIN Account a ON a.CustomerID = c.CustomerID "
	    		+ "JOIN Trade tr ON tr.AccountID = a.AccountID "
	    		+ "JOIN `Transaction` tx ON tx.TransactionID = tr.TransactionID "
	    		+ "GROUP BY p.SSN, p.FirstName, p.LastName, p.Email, p.Address, p.City, p.State, p.ZipCode, p.Telephone, c.Rating, c.CardNumber "
	    		+ "ORDER BY TotalFeesPaid DESC LIMIT 1";

	    try (Connection conn = DatabaseConnection.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql);
	         ResultSet rs = ps.executeQuery()) {

	        if (rs.next()) {
	            Customer customer = new Customer();
	            Location location = new Location();

	            customer.setSsn(rs.getString("SSN"));
	            customer.setClientId(rs.getString("SSN"));
	            customer.setId(rs.getString("SSN"));
	            customer.setFirstName(rs.getString("FirstName"));
	            customer.setLastName(rs.getString("LastName"));
	            customer.setEmail(rs.getString("Email"));
	            customer.setAddress(rs.getString("Address"));
	            customer.setTelephone(rs.getString("Telephone"));
	            customer.setCreditCard(rs.getString("Cardnumber"));
	            customer.setRating(rs.getInt("Rating"));

	            location.setCity(rs.getString("City"));
	            location.setState(rs.getString("State"));
	            location.setZipCode(rs.getInt("ZipCode"));

	            customer.setLocation(location);

	            return customer;
	        }

	    } catch (SQLException e) {
	        e.printStackTrace();
	    }

	    return null;
	}

	/*
	 * This method fetches the customer details and returns it
	 * customerID, which is the Customer's ID who's details have to be fetched, is given as method parameter
	 * The students code to fetch data from the database will be written here
	 * The customer record is required to be encapsulated as a "Customer" class object
	 */
	public Customer getCustomer(String customerID) {
	    String sql = "SELECT p.ssn, p.firstname, p.lastname, p.email, p.address, p.city, p.state, p.zipcode, p.telephone, " +
	                 "c.customerid, c.rating, c.cardnumber, " +
	                 "a.accountid, a.accountcreated " +
	                 "FROM customer c " +
	                 "JOIN person p ON c.customerid = p.ssn " +
	                 "JOIN account a ON c.customerid = a.customerid " +
	                 "WHERE c.customerid = ?";

	    try (Connection conn = DatabaseConnection.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql)) {

	        ps.setString(1, customerID);
	        ResultSet rs = ps.executeQuery();

	        if (rs.next()) {
	            Customer customer = new Customer();
	            Location location = new Location();

	            // Personal Information
	            customer.setSsn(rs.getString("ssn"));
	            customer.setFirstName(rs.getString("firstname"));
	            customer.setId(rs.getString("ssn"));
	            customer.setLastName(rs.getString("lastname"));
	            customer.setEmail(rs.getString("email"));
	            customer.setAddress(rs.getString("address"));
	            customer.setTelephone(rs.getString("telephone"));

	            // Customer Info
	            customer.setClientId(rs.getString("customerid"));
	            customer.setCreditCard(rs.getString("cardnumber"));
	            customer.setRating(rs.getInt("rating"));

	            // Location
	            location.setCity(rs.getString("city"));
	            location.setState(rs.getString("state"));
	            location.setZipCode(rs.getInt("zipcode"));
	            customer.setLocation(location);

	            // Account Info
	            customer.setAccountNumber(rs.getInt("accountid"));
	            Date accountCreated = rs.getDate("accountcreated");
	            if (accountCreated != null) {
	                customer.setAccountCreationTime(new SimpleDateFormat("yyyy-MM-dd").format(accountCreated));
	            }
	            
	            return customer;
	        }

	    } catch (SQLException e) {
	        e.printStackTrace();
	    }

	    return null;
	}

    /*
     * Deletes a customer and their associated account and person records.
     * Assumes customerID is used as the foreign key across all three tables.
     */
	public String deleteCustomer(String customerID) {
	    String sqlDeleteCustomer = "DELETE FROM Customer WHERE CustomerID = ?";
	    String sqlDeletePerson = "DELETE FROM Person WHERE SSN = ?";

	    try (Connection conn = DatabaseConnection.getConnection()) {
	        conn.setAutoCommit(false); // Start transaction

	        // Delete Customer
	        try (PreparedStatement psDelCustomer = conn.prepareStatement(sqlDeleteCustomer)) {
	        	psDelCustomer.setString(1, customerID);
	        	psDelCustomer.executeUpdate();
	        }

	        // Delete Person (ssn == customerID)
	        try (PreparedStatement psDelPerson = conn.prepareStatement(sqlDeletePerson)) {
	        	psDelPerson.setString(1, customerID);
	        	psDelPerson.executeUpdate();
	        }

	        conn.commit();
	        return "success";

	    } catch (SQLException e) {
	        e.printStackTrace();
	        return "failure";
	    }
	}

	/*
	 * This method returns the Customer's ID based on the provided email address
	 * The students code to fetch data from the database will be written here
	 * username, which is the email address of the customer, who's ID has to be returned, is given as method parameter
	 * The Customer's ID is required to be returned as a String
	 */
	public String getCustomerID(String email) {
	    String sql = "SELECT c.customerid " +
	                 "FROM customer c JOIN person p ON c.customerid = p.ssn " +
	                 "WHERE p.email = ?";

	    try (Connection conn = DatabaseConnection.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql)) {

	        ps.setString(1, email);
	        ResultSet rs = ps.executeQuery();

	        if (rs.next()) {
	            return rs.getString("customerid");
	        } else {
	            return null;
	        }

	    } catch (SQLException e) {
	        e.printStackTrace();
	        return null;
	    }
	}


	/*
	 * All the values of the add customer form are encapsulated in the customer object.
	 * These can be accessed by getter methods (see Customer class in model package).
	 * e.g. firstName can be accessed by customer.getFirstName() method.
	 * The sample code returns "success" by default.
	 * You need to handle the database insertion of the customer details and return "success" or "failure" based on result of the database insertion.
	 */
	public String addCustomer(Customer customer) {
		String checkPersonSQL = "SELECT ssn FROM person WHERE ssn = ?";
	    String insertPersonSQL = "INSERT INTO person (SSN, FirstName, LastName, Email, Address, City, State, Zipcode, Telephone) " +
	                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	    String insertCustomerSQL = "INSERT INTO customer (CustomerID, Rating, CardNumber) " +
	                               "VALUES (?, ?, ?)";
	    String insertAccountSQL = "INSERT INTO account (CustomerID, AccountCreated) " +
	                               "VALUES (?, ?)";

	    try (Connection conn = DatabaseConnection.getConnection()) {
	        conn.setAutoCommit(false); // Start transaction

	        try (
	            PreparedStatement checkPersonStmt = conn.prepareStatement(checkPersonSQL);
	            PreparedStatement insertPersonStmt = conn.prepareStatement(insertPersonSQL);
	            PreparedStatement insertCustomerStmt = conn.prepareStatement(insertCustomerSQL);
	            PreparedStatement insertAccountStmt = conn.prepareStatement(insertAccountSQL)
	        ) {
	            // Check if person already exists
	            checkPersonStmt.setString(1, customer.getSsn());
	            ResultSet rs = checkPersonStmt.executeQuery();
	            boolean personExists = rs.next();
	            rs.close();

	            // Insert person only if not already in DB
	            if (!personExists) {
	                insertPersonStmt.setString(1, customer.getSsn());
	                insertPersonStmt.setString(2, customer.getFirstName());
	                insertPersonStmt.setString(3, customer.getLastName());
	                insertPersonStmt.setString(4, customer.getEmail());
	                insertPersonStmt.setString(5, customer.getAddress());
	                insertPersonStmt.setString(6, customer.getLocation().getCity());
	                insertPersonStmt.setString(7, customer.getLocation().getState());
	                insertPersonStmt.setInt(8, customer.getLocation().getZipCode());
	                insertPersonStmt.setString(9, customer.getTelephone());
	                insertPersonStmt.executeUpdate();
	            }

	            // Insert into customer (assume 1 customer per person)
	            insertCustomerStmt.setString(1, customer.getId());
	            insertCustomerStmt.setInt(2, customer.getRating());
	            insertCustomerStmt.setString(3, customer.getCreditCard());
	            insertCustomerStmt.executeUpdate();

	            // Insert into Account
	            insertAccountStmt.setString(1, customer.getId());
	            java.sql.Date sqlDate = new java.sql.Date(new java.util.Date().getTime());
	            insertAccountStmt.setDate(2, sqlDate);
	            insertAccountStmt.executeUpdate();

	            conn.commit();
	            return "success";

	        } catch (SQLException e) {
	            conn.rollback();
	            e.printStackTrace();
	            return "failure";
	        }

	    } catch (SQLException e) {
	        e.printStackTrace();
	        return "failure";
	    }
	}

	public String editCustomer(Customer customer) {
		String updatePersonSQL = "UPDATE person SET firstname = ?, lastname = ?, address = ?, city = ?, state = ?, zipcode = ?, telephone = ?, email = ? WHERE ssn = ?";
		String updateCustomerSQL = "UPDATE customer SET cardnumber = ?, rating = ? WHERE customerid = ?";

		try (Connection conn = DatabaseConnection.getConnection()) {
			conn.setAutoCommit(false); // Start transaction

			try (
				PreparedStatement personStmt = conn.prepareStatement(updatePersonSQL);
				PreparedStatement customerStmt = conn.prepareStatement(updateCustomerSQL)
			) {
				// Update person
				personStmt.setString(1, customer.getFirstName());
				personStmt.setString(2, customer.getLastName());
				personStmt.setString(3, customer.getAddress());
				personStmt.setString(4, customer.getLocation().getCity());
				personStmt.setString(5, customer.getLocation().getState());
				personStmt.setInt(6, customer.getLocation().getZipCode());
				personStmt.setString(7, customer.getTelephone());
				personStmt.setString(8, customer.getEmail());
				personStmt.setString(9, customer.getSsn());
				int personRows = personStmt.executeUpdate();

				// Update customer
				customerStmt.setString(1, customer.getCreditCard());
				customerStmt.setInt(2, customer.getRating());
				customerStmt.setString(3, customer.getSsn()); // customerID
				int customerRows = customerStmt.executeUpdate();

				if (personRows > 0 && customerRows > 0) {
					conn.commit();
					return "success";
				} else {
					conn.rollback();
					return "failure";
				}

			} catch (SQLException e) {
				conn.rollback();
				e.printStackTrace();
				return "failure";
			}

		} catch (SQLException e) {
			e.printStackTrace();
			return "failure";
		}
	}


    /*
     * This method fetches all customer mailing details and returns them
     * as a List of Customer objects with just name, address, email, and location fields.
     */
	public List<Customer> getCustomerMailingList() {
	    List<Customer> customers = new ArrayList<>();

	    String sql = "SELECT p.ssn, p.firstname, p.lastname, p.address, p.city, p.state, p.zipcode, p.email " +
	                 "FROM customer c " +
	                 "JOIN person p ON c.customerid = p.ssn";

	    try (Connection conn = DatabaseConnection.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql);
	         ResultSet rs = ps.executeQuery()) {

	        while (rs.next()) {
	            Customer customer = new Customer();
	            Location location = new Location();

	            customer.setId(rs.getString("ssn"));
	            customer.setSsn(rs.getString("ssn"));
	            customer.setFirstName(rs.getString("firstname"));
	            customer.setLastName(rs.getString("lastname"));
	            customer.setAddress(rs.getString("address"));
	            customer.setEmail(rs.getString("email"));

	            location.setCity(rs.getString("city"));
	            location.setState(rs.getString("state"));
	            location.setZipCode(rs.getInt("zipcode"));

	            customer.setLocation(location);

	            customers.add(customer);
	        }

	    } catch (SQLException e) {
	        e.printStackTrace();
	    }

	    return customers;
	}

    public List<Customer> getAllCustomers() {
        List<Customer> customers = new ArrayList<>();

        String sql = "SELECT p.ssn, p.firstname, p.lastname, p.email, " +
                     "p.address, p.city, p.state, p.zipcode, p.telephone, " +
                     "c.customerid, c.rating, c.cardnumber " +
                     "FROM customer c " +
                     "JOIN person p ON c.customerid = p.ssn";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Customer customer = new Customer();
                Location location = new Location();

                customer.setSsn(rs.getString("ssn"));
                customer.setClientId(rs.getString("customerid"));
                customer.setFirstName(rs.getString("firstname"));
                customer.setLastName(rs.getString("lastname"));
                customer.setEmail(rs.getString("email"));
                customer.setAddress(rs.getString("address"));
                customer.setTelephone(rs.getString("telephone"));
                customer.setCreditCard(rs.getString("cardnumber"));
                customer.setRating(rs.getInt("rating"));

                location.setCity(rs.getString("city"));
                location.setState(rs.getString("state"));
                location.setZipCode(rs.getInt("zipcode"));

                customer.setLocation(location);
                customers.add(customer);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return customers;
    }
}
