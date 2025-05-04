package dao;

import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.math.BigDecimal;

import model.Customer;
import model.Employee;
import model.Location;

public class EmployeeDao {
	/*
	 * This class handles all the database operations related to the employee table
	 */
	
  public static void main(String[] args) {
	  EmployeeDao e = new EmployeeDao();
      System.out.println(e.addEmployee(e.getDummyEmployee()));
  }

    public Employee getDummyEmployee()
    {
        Employee employee = new Employee();

        Location location = new Location();
        location.setCity("Stony Brook");
        location.setState("NY");
        location.setZipCode(11790);

		/*Sample data begins*/
        employee.setSsn("123456789");
        employee.setLevel("Manager");
        employee.setEmail("shiyong@cs.sunysb.edu");
        employee.setFirstName("Shiyong");
        employee.setLastName("Lu");
        employee.setLocation(location);
        employee.setAddress("123 Success Street");
        employee.setStartDate("2006-10-17");
        employee.setTelephone("5166328959");
        employee.setEmployeeID("631-413-5555");
        employee.setHourlyRate(100);
		/*Sample data ends*/

        return employee;
    }

    public List<Employee> getDummyEmployees()
    {
       List<Employee> employees = new ArrayList<Employee>();

        for(int i = 0; i < 10; i++)
        {
            employees.add(getDummyEmployee());
        }

        return employees;
    }

	public String addEmployee(Employee employee) {
		
		/*
		 * All the values of the add employee form are encapsulated in the employee object.
		 * These can be accessed by getter methods (see Employee class in model package).
		 * e.g. firstName can be accessed by employee.getFirstName() method.
		 * The sample code returns "success" by default.
		 * You need to handle the database insertion of the employee details and return "success" or "failure" based on result of the database insertion.
		 */
		
		String sqlCheckPerson = "SELECT 1 FROM Person WHERE SSN = ?";
        String sqlPerson = "INSERT INTO Person (SSN, FirstName, LastName, Email, Address, City, State, ZipCode, Telephone) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String sqlEmployee = "INSERT INTO Employee (SSN, StartDate, HourlyRate, Level) VALUES (?, ?, ?, ?)";
        
        // Connect to the database
        try (Connection conn = DatabaseConnection.getConnection()) {
        	
        	// Don't commit anything yet before it's ready to send out.
            conn.setAutoCommit(false);
            
            int ssn = Integer.parseInt(employee.getSsn());
            
            // First check, if the person exists in the database to avoid creating another person
            boolean personExists;
            try (PreparedStatement psCheck = conn.prepareStatement(sqlCheckPerson)) {
            	// change the first ? into the ssn
                psCheck.setInt(1, ssn);
                
                // Sets personExist if we get a response
                try (ResultSet rs = psCheck.executeQuery()) {
                    personExists = rs.next();
                }
            }
            
            // Insert into the Person table only if it doesn't exist
            if (!personExists) {
                try (PreparedStatement psPerson = conn.prepareStatement(sqlPerson)) {
                    psPerson.setInt(1, ssn);
                    psPerson.setString(2, employee.getFirstName());
                    psPerson.setString(3, employee.getLastName());
                    psPerson.setString(4, employee.getEmail());
                    psPerson.setString(5, employee.getAddress());
                    
                    Location loc = employee.getLocation();
                    psPerson.setString(6, loc.getCity());
                    psPerson.setString(7, loc.getState());
                    psPerson.setInt(8, loc.getZipCode());
                    
                    psPerson.setString(9, employee.getTelephone());
                    
                    // execute the sql statement
                    psPerson.executeUpdate();
                }
            }
            
            // Insert into Employee table, and get the auto generated key the database generated
            try (PreparedStatement psEmp = conn.prepareStatement(sqlEmployee, Statement.RETURN_GENERATED_KEYS)) {
                psEmp.setInt(1, ssn);
                psEmp.setDate(2, Date.valueOf(employee.getStartDate()));
                psEmp.setBigDecimal(3, BigDecimal.valueOf(employee.getHourlyRate()).setScale(2)); // Use BigDecimal to preserve cents and to send a precise decimal
                psEmp.setString(4, employee.getLevel());
                psEmp.executeUpdate();
                
                // Get the generated key from the database and set the ID to that number
                try (ResultSet rs = psEmp.getGeneratedKeys()) {
                    if (rs.next()) {
                        employee.setEmployeeID(String.valueOf(rs.getInt(1)));
                    }
                }
            }
            
            // Have to manually commit the sql statements
            conn.commit();
            return "success";
           
        // Failure if connecting to the database or any of the sql statements fail because the error bubbles up
        } catch (Exception e) {
            e.printStackTrace();
            return "failure";
        }
    }

	public String editEmployee(Employee employee) {
		/*
		 * All the values of the edit employee form are encapsulated in the employee object.
		 * These can be accessed by getter methods (see Employee class in model package).
		 * e.g. firstName can be accessed by employee.getFirstName() method.
		 * The sample code returns "success" by default.
		 * You need to handle the database update and return "success" or "failure" based on result of the database update.
		 */
		
		/*Sample data begins*/
		return "success";
		/*Sample data ends*/

	}

	public String deleteEmployee(String employeeID) {
		/*
		 * employeeID, which is the Employee's ID which has to be deleted, is given as method parameter
		 * The sample code returns "success" by default.
		 * You need to handle the database deletion and return "success" or "failure" based on result of the database deletion.
		 */
		
		/*Sample data begins*/
		return "success";
		/*Sample data ends*/

	}

	
	public List<Employee> getEmployees(String searchKeyword) {

		/*
		 * The students code to fetch data from the database will be written here
		 * Query to return details about all the employees must be implemented
		 * Each record is required to be encapsulated as a "Employee" class object and added to the "employees" List
		 */

		List<Employee> employees = new ArrayList<Employee>();

//		Location location = new Location();
//		location.setCity("Stony Brook");
//		location.setState("NY");
//		location.setZipCode(11790);

		/*Sample data begins*/
//		for (int i = 0; i < 10; i++) {
//			Employee employee = new Employee();
//			employee.setId("111-11-1111");
//			employee.setEmail("shiyong@cs.sunysb.edu");
//			employee.setFirstName("Shiyong");
//			employee.setLastName("Lu");
//			employee.setAddress("123 Success Street");
//			employee.setLocation(location);
//			employee.setTelephone("5166328959");
//			employee.setEmployeeID("631-413-5555");
//			employee.setHourlyRate(100);
//			employees.add(employee);
//		}		
		/*Sample data ends*/
		
		Connection con = null;
		Statement st = null;
		ResultSet rs = null;
		
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost:3306/demo", "root", "root");
			st = con.createStatement();
			
			String query = "SELECT * FROM employee WHERE email LIKE '%" + searchKeyword + "%'";
			rs = st.executeQuery(query);
			
			while (rs.next()) {
				Employee employee = new Employee();
				
				employee.setEmail(rs.getString("email"));
				employee.setFirstName(rs.getString("firstName"));
				employee.setLastName(rs.getString("lastname"));
				employee.setEmployeeRole(rs.getString("role"));
				employee.setAddress(rs.getString("address"));
				employee.setStartDate(rs.getString("startDate"));
				employee.setState(rs.getString("state"));
				employee.setZipcode(rs.getInt("zipcode"));
				employee.setTelephone(rs.getString("telephone"));
				employee.setEmployeeID(rs.getString("employeeID"));
				employee.setHourlyRate(rs.getFloat("hourlyRate"));
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		
		return employees;
	}

	public Employee getEmployee(String employeeID) {

		/*
		 * The students code to fetch data from the database based on "employeeID" will be written here
		 * employeeID, which is the Employee's ID who's details have to be fetched, is given as method parameter
		 * The record is required to be encapsulated as a "Employee" class object
		 */

		return getDummyEmployee();
	}
	
	public Employee getHighestRevenueEmployee() {
		
		/*
		 * The students code to fetch employee data who generated the highest revenue will be written here
		 * The record is required to be encapsulated as a "Employee" class object
		 */
		
		return getDummyEmployee();
	}

	public String getEmployeeID(String username) {
		/*
		 * The students code to fetch data from the database based on "username" will be written here
		 * username, which is the Employee's email address who's Employee ID has to be fetched, is given as method parameter
		 * The Employee ID is required to be returned as a String
		 */

		return "111-11-1111";
	}

}
