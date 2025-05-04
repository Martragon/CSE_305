package dao;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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

        Customer customer = new Customer();
        customer.setId("111111111");
        customer.setSsn("111111111");
        customer.setAddress("123 Success Street");
        customer.setLastName("Lu");
        customer.setFirstName("Shiyong");
        customer.setEmail("shiyong@cs.sunysb.edu");
        customer.setLocation(location);
        customer.setTelephone("5166328959");
        customer.setCreditCard("1234567812345678");
        customer.setRating(1);

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
	public List<Customer> getCustomers(String searchKeyword) {
		/*
		 * This method fetches one or more customers based on the searchKeyword and returns it as an ArrayList
		 *
		 * The students code to fetch data from the database based on searchKeyword will be written here
		 * Each record is required to be encapsulated as a "Customer" class object and added to the "customers" List
		 */
	    List<Customer> customers = new ArrayList<>();

	    String sql = "SELECT p.ssn, p.firstname, p.lastname, p.email, " +
	                 "p.address, p.city, p.state, p.zipcode, p.telephone, " +
	                 "c.customerid, c.rating, c.cardnumber " +
	                 "FROM customer c " +
	                 "JOIN person p ON c.ssn = p.ssn " +
	                 "WHERE p.email LIKE ?";

	    try (Connection conn = DatabaseConnection.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql)) {

	        ps.setString(1, "%" + searchKeyword + "%");
	        ResultSet rs = ps.executeQuery();

	        while (rs.next()) {
	            Customer customer = new Customer();
	            Location location = new Location();

//	            customer.setSsn(rs.getString("ssn"));
//	            customer.setId(rs.getString("customerid")); // if needed
//	            customer.setFirstName(rs.getString("firstname"));
//	            customer.setLastName(rs.getString("lastname"));
//	            customer.setEmail(rs.getString("email"));
//	            customer.setAddress(rs.getString("address"));
//	            customer.setTelephone(rs.getString("telephone"));
//	            customer.setCreditCard(rs.getString("cardnumber"));
//	            customer.setRating(rs.getInt("rating"));
//
//	            location.setCity(rs.getString("city"));
//	            location.setState(rs.getString("state"));
//	            location.setZipCode(rs.getInt("zipcode"));
//
//	            customer.setLocation(location);
	            customers.add(customer);
	        }

	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	    
	    return getDummyCustomerList();
	}


	public Customer getHighestRevenueCustomer() {
		/*
		 * This method fetches the customer who generated the highest total revenue and returns it
		 * The students code to fetch data from the database will be written here
		 * The customer record is required to be encapsulated as a "Customer" class object
		 */
		String sql = "SELECT p.ssn, p.firstname, p.lastname, p.email, p.address, p.city, p.state, p.zipcode, p.telephone, " +
				"c.customerid, c.rating, c.cardnumber, " +
				"SUM(b.price * b.quantity) AS total_revenue " +
				"FROM customer c " +
				"JOIN person p ON c.ssn = p.ssn " +
				"JOIN buy b ON c.customerid = b.customerid " +
				"GROUP BY c.customerid " +
				"ORDER BY total_revenue DESC " +
				"LIMIT 1";

		try (Connection conn = DatabaseConnection.getConnection();
				PreparedStatement ps = conn.prepareStatement(sql);
				ResultSet rs = ps.executeQuery()) {

			if (rs.next()) {
				Customer customer = new Customer();
				Location location = new Location();

				customer.setSsn(rs.getString("ssn"));
				customer.setId(rs.getString("customerid"));
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
	
				return customer;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	public Customer getCustomer(String customerID) {
		/*
		 * This method fetches the customer details and returns it
		 * customerID, which is the Customer's ID who's details have to be fetched, is given as method parameter
		 * The students code to fetch data from the database will be written here
		 * The customer record is required to be encapsulated as a "Customer" class object
		 */
		String sql = "SELECT * " +
				"FROM customer c JOIN person p ON c.ssn = p.ssn " +
				"WHERE c.customerid = ?";

		try (Connection conn = DatabaseConnection.getConnection();
				PreparedStatement ps = conn.prepareStatement(sql)) {
	
			ps.setString(1, customerID);
			ResultSet rs = ps.executeQuery();
	
			if (rs.next()) {
				Customer customer = new Customer();
				Location location = new Location();

				customer.setSsn(rs.getString("ssn"));
				customer.setId(rs.getString("customerid"));
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
				
				return customer;
			} else {
				return null;
			}
	
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public String deleteCustomer(String customerID) {

		/*
		 * This method deletes a customer returns "success" string on success, else returns "failure"
		 * The students code to delete the data from the database will be written here
		 * customerID, which is the Customer's ID who's details have to be deleted, is given as method parameter
		 */

		/*Sample data begins*/
		return "success";
		/*Sample data ends*/
		
	}


	public String getCustomerID(String email) {
		/*
		 * This method returns the Customer's ID based on the provided email address
		 * The students code to fetch data from the database will be written here
		 * username, which is the email address of the customer, who's ID has to be returned, is given as method parameter
		 * The Customer's ID is required to be returned as a String
		 */
	    String sql = "SELECT c.customerid " +
	                 "FROM customer c JOIN person p ON c.ssn = p.ssn " +
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


	public String addCustomer(Customer customer) {
		/*
		 * All the values of the add customer form are encapsulated in the customer object.
		 * These can be accessed by getter methods (see Customer class in model package).
		 * e.g. firstName can be accessed by customer.getFirstName() method.
		 * The sample code returns "success" by default.
		 * You need to handle the database insertion of the customer details and return "success" or "failure" based on result of the database insertion.
		 */
		String checkPersonSQL = "SELECT ssn FROM person WHERE ssn = ?";
	    String insertPersonSQL = "INSERT INTO person (SSN, FirstName, LastName, Email, Address, City, State, Zipcode, Telephone) " +
	                             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	    String insertCustomerSQL = "INSERT INTO customer (CustomerID, SSN, Rating, CardNumber) " +
	                               "VALUES (?, ?, ?, ?)";

	    try (Connection conn = DatabaseConnection.getConnection()) {
	        conn.setAutoCommit(false); // Start transaction

	        try (
	            PreparedStatement checkPersonStmt = conn.prepareStatement(checkPersonSQL);
	            PreparedStatement insertPersonStmt = conn.prepareStatement(insertPersonSQL);
	            PreparedStatement insertCustomerStmt = conn.prepareStatement(insertCustomerSQL)
	        ) {
	            // 1. Check if person already exists
	            checkPersonStmt.setString(1, customer.getSsn());
	            ResultSet rs = checkPersonStmt.executeQuery();
	            boolean personExists = rs.next();
	            rs.close();

	            // 2. Insert person only if not already in DB
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

	            // 3. Insert into customer (assume 1 customer per person)
	            insertCustomerStmt.setString(2, customer.getSsn());
	            insertCustomerStmt.setInt(3, customer.getRating());
	            insertCustomerStmt.setString(4, customer.getCreditCard());
	            insertCustomerStmt.executeUpdate();

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
		/*
		 * All the values of the edit customer form are encapsulated in the customer object.
		 * These can be accessed by getter methods (see Customer class in model package).
		 * e.g. firstName can be accessed by customer.getFirstName() method.
		 * The sample code returns "success" by default.
		 * You need to handle the database update and return "success" or "failure" based on result of the database update.
		 */
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
				customerStmt.setString(3, customer.getId()); // customerId
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

    public List<Customer> getCustomerMailingList() {
		/*
		 * This method fetches the all customer mailing details and returns it
		 * The students code to fetch data from the database will be written here
		 */
        List<Customer> customers = new ArrayList<>();

        String sql = "SELECT p.ssn, p.firstname, p.lastname, p.address, p.city, p.state, p.zipcode, p.email " +
                     "FROM customer c " +
                     "JOIN person p ON c.ssn = p.ssn";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Customer customer = new Customer();
                Location location = new Location();

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
                System.out.println(customer.getSsn());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return customers;
    }

    public List<Customer> getAllCustomers() {
        /*
		 * This method fetches returns all customers
		 */
        return getDummyCustomerList();
    }
    
    public static void main(String[] args) {
        
        CustomerDao dao = new CustomerDao();
        Customer test = dao.getCustomer("1");
        System.out.println(test.getFirstName());

    }
}
