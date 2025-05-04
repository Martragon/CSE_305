package dao;

import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import model.Customer;
import model.Employee;
import model.Location;

public class EmployeeDao {
	/*
	 * This class handles all the database operations related to the employee table
	 */

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
        String sqlEmployee = "INSERT INTO Employee (EmployeeID, StartDate, HourlyRate, Level) VALUES (?, ?, ?, ?)";
        
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
            
            // Insert into Employee table
            try (PreparedStatement psEmp = conn.prepareStatement(sqlEmployee)) {
            	// The EmployeeID is equal to the SSN
                psEmp.setInt(1, ssn);
                psEmp.setDate(2, Date.valueOf(employee.getStartDate()));
                psEmp.setFloat(3, employee.getHourlyRate());
                psEmp.setString(4, employee.getLevel());
                psEmp.executeUpdate();
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
		
		String sqlPerson = "UPDATE Person SET FirstName = ?, LastName = ?, Address = ?, Email = ?, City = ?, State = ?, ZipCode = ?, Telephone = ? WHERE SSN = ?";
	    String sqlEmployee = "UPDATE Employee SET StartDate = ?, HourlyRate = ?, Level = ? WHERE EmployeeID = ?";
	    
	    // Connect to the database
	    try (Connection conn = DatabaseConnection.getConnection()) {
	    	
		// Don't commit anything yet before it's ready to send out.
		conn.setAutoCommit(false);
		
			// Update the Person table
			try (PreparedStatement psPerson = conn.prepareStatement(sqlPerson)) {
				// Update the information
				psPerson.setString(1, employee.getFirstName());
				psPerson.setString(2, employee.getLastName());
				psPerson.setString(3, employee.getAddress());
				psPerson.setString(4, employee.getEmail());
				
				Location loc = employee.getLocation();
				psPerson.setString(5, loc.getCity());
				psPerson.setString(6, loc.getState());
				psPerson.setInt(7, loc.getZipCode());
				psPerson.setString(8, employee.getTelephone());
				
				// Where SSN = ?
			    psPerson.setInt(9, Integer.parseInt(employee.getSsn()));
			    
			    psPerson.executeUpdate();
			}
	        
	        // Update Employee
	        try (PreparedStatement psEmp = conn.prepareStatement(sqlEmployee)) {
	            psEmp.setDate(1, Date.valueOf(employee.getStartDate()));
	            psEmp.setFloat(2, employee.getHourlyRate());
	            psEmp.setString(3, employee.getLevel());
	            
	            // Where Employee ID = ?
	            psEmp.setInt(4, Integer.parseInt(employee.getEmployeeID()));
	            
	            psEmp.executeUpdate();
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

	public String deleteEmployee(String employeeID) {
		/*
		 * employeeID, which is the Employee's ID which has to be deleted, is given as method parameter
		 * The sample code returns "success" by default.
		 * You need to handle the database deletion and return "success" or "failure" based on result of the database deletion.
		 */
		
		String sql = "DELETE FROM Employee WHERE EmployeeID = ?";
		
		// Connect to the Database and prepare the sql statement
		try (Connection conn = DatabaseConnection.getConnection(); 
				PreparedStatement ps = conn.prepareStatement(sql)) {
			
			// Where EmployeeID = ?
            ps.setInt(1, Integer.parseInt(employeeID));
            
            // Execute the statement and get the number of rows returned
            int rows = ps.executeUpdate();
            
            // If something was returned, it means it was successful 
            return rows > 0 ? "success" : "failure";
        
        // In any error, return "failure"
        } catch (Exception e) {
            e.printStackTrace();
            return "failure";
        }

	}

	// The search keyword pertains to the email of an employee
	public List<Employee> getEmployees(String searchKeyword) {

		/*
		 * The students code to fetch data from the database will be written here
		 * Query to return details about all the employees must be implemented
		 * Each record is required to be encapsulated as a "Employee" class object and added to the "employees" List
		 */

		List<Employee> employees = new ArrayList<Employee>();
		String sql = "SELECT p.SSN, p.FirstName, p.LastName, p.Email, p.Address, p.City, p.State, p.ZipCode, p.Telephone, e.EmployeeID, e.StartDate, e.HourlyRate, e.Level FROM Person p JOIN Employee e ON p.SSN = e.EmployeeID WHERE email LIKE %?%";
		
		// Connect to the database and prepare the statement
		try (Connection conn = DatabaseConnection.getConnection();
			PreparedStatement ps = conn.prepareStatement(sql)){
			
			// WHERE email LIKE %?%
			ps.setString(1, searchKeyword);
			
			// Execute the query and keep getting the next row
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					Employee employee = new Employee();
					
					employee.setSsn(rs.getString("SSN"));
					employee.setFirstName(rs.getString("FirstName"));
					employee.setLastName(rs.getString("LastName"));
					employee.setEmail(rs.getString("Email"));
					employee.setAddress(rs.getString("Address"));
					
					Location loc = new Location();
					loc.setCity(rs.getString("City"));
					loc.setState(rs.getString("State"));
					loc.setZipCode(rs.getInt("ZipCode"));
					employee.setLocation(loc);
					
					employee.setTelephone(rs.getString("Telephone"));
					employee.setEmployeeID(String.valueOf(rs.getInt("EmployeeID")));
					employee.setStartDate(rs.getDate("StartDate").toString());
					employee.setHourlyRate(rs.getFloat("HourlyRate"));
					employee.setLevel(rs.getString("Level"));
					
					employees.add(employee);
				}	
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		
		return employees;
	}
	
	// returns all employees
	public List<Employee> getEmployees() {
        String sql = "SELECT p.SSN, p.FirstName, p.LastName, p.Email, p.Address, p.City, p.State, p.ZipCode, p.Telephone, e.EmployeeID, e.StartDate, e.HourlyRate, e.Level FROM Person p JOIN Employee e ON p.SSN = e.EmployeeID";
        
        List<Employee> list = new ArrayList<>();
        
        // Connect to the database, prepare the statement and get the result set
        // Don't need to fill in ? for sql or have multiple different queries so we can do it in one try statement
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
        	
        	// while there are rows to be read, keep on creating the employee object and add it to the list
            while (rs.next()) {
                Employee employee = new Employee();
                employee.setSsn(rs.getString("SSN"));
                employee.setFirstName(rs.getString("FirstName"));
                employee.setLastName(rs.getString("LastName"));
                employee.setEmail(rs.getString("Email"));
                employee.setAddress(rs.getString("Address"));
                
                Location loc = new Location();
                loc.setCity(rs.getString("City"));
                loc.setState(rs.getString("State"));
                loc.setZipCode(rs.getInt("ZipCode"));
                employee.setLocation(loc);
                
                employee.setTelephone(rs.getString("Telephone"));
                employee.setEmployeeID(String.valueOf(rs.getInt("EmployeeID")));
                employee.setStartDate(rs.getDate("StartDate").toString());
                employee.setHourlyRate(rs.getFloat("HourlyRate"));
                employee.setLevel(rs.getString("Level"));
                
                list.add(employee);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return list;
    }

	public Employee getEmployee(String employeeID) {

		/*
		 * The students code to fetch data from the database based on "employeeID" will be written here
		 * employeeID, which is the Employee's ID who's details have to be fetched, is given as method parameter
		 * The record is required to be encapsulated as a "Employee" class object
		 */

		String sql = "SELECT p.SSN, p.FirstName, p.LastName, p.Email, p.Address, p.City, p.State, p.ZipCode, p.Telephone, e.EmployeeID, e.StartDate, e.HourlyRate, e.Level FROM Person p JOIN Employee e ON p.SSN = e.EmployeeID WHERE e.EmployeeID = ?";
		
		// Connect to the database and prepare the statement
		try (Connection conn = DatabaseConnection.getConnection();
			PreparedStatement ps = conn.prepareStatement(sql)){
			
			// WHERE e.EmployeeID = ?
			ps.setString(1, employeeID);
			
			// Execute the query and keep getting the next row
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					Employee employee = new Employee();
					
					employee.setSsn(rs.getString("SSN"));
					employee.setFirstName(rs.getString("FirstName"));
					employee.setLastName(rs.getString("LastName"));
					employee.setEmail(rs.getString("Email"));
					employee.setAddress(rs.getString("Address"));
					
					Location loc = new Location();
					loc.setCity(rs.getString("City"));
					loc.setState(rs.getString("State"));
					loc.setZipCode(rs.getInt("ZipCode"));
					employee.setLocation(loc);
					
					employee.setTelephone(rs.getString("Telephone"));
					employee.setEmployeeID(String.valueOf(rs.getInt("EmployeeID")));
					employee.setStartDate(rs.getDate("StartDate").toString());
					employee.setHourlyRate(rs.getFloat("HourlyRate"));
					employee.setLevel(rs.getString("Level"));
					
					return employee;
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		
		// return nothing on error
		return null;
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

		String sql = "SELECT e.EmployeeID FROM Person p JOIN Employee e ON p.SSN = e.EmployeeID WHERE p.Email = ?";
		
		// Connect to the database and prepare the statement
		try (Connection conn = DatabaseConnection.getConnection();
			PreparedStatement ps = conn.prepareStatement(sql)){
			
			// WHERE e.EmployeeID = ?
			ps.setString(1, username);
			
			// Execute the query and keep getting the next row
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getString("EmployeeID");
				} else {
					return null;
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		
		// return nothing on error
		return null;
	}
}
