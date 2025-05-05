package dao;

import java.sql.*;
import model.Login;

public class LoginDao {
	/*
	 * This class handles all the database operations related to login functionality
	 */
	
	/*
	 * Return a Login object with role as "manager", "customerRepresentative" or "customer" if successful login
	 * Else, return null
	 * The role depends on the type of the user, which has to be handled in the database
	 * username, which is the email address of the user, is given as method parameter
	 * password, which is the password of the user, is given as method parameter
	 * Query to verify the username and password and fetch the role of the user, must be implemented
	 */
	public Login login(String username, String password, String role) {
	    String sql = "SELECT * FROM login WHERE username = ? AND password = ? AND role = ?";

	    try (Connection conn = DatabaseConnection.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql)) {

	        ps.setString(1, username);
	        ps.setString(2, password);
	        ps.setString(3, role);

	        ResultSet rs = ps.executeQuery();

	        if (rs.next()) {
	            Login login = new Login();
	            login.setUsername(rs.getString("username"));
	            login.setPassword(rs.getString("password"));
	            login.setRole(rs.getString("role"));
	            return login;
	        }

	    } catch (SQLException e) {
	        e.printStackTrace();
	    }

	    return null;
	}

	/*
	 * Query to insert a new record for user login must be implemented
	 * login, which is the "Login" Class object containing username and password for the new user, is given as method parameter
	 * The username and password from login can get accessed using getter methods in the "Login" model
	 * e.g. getUsername() method will return the username encapsulated in login object
	 * Return "success" on successful insertion of a new user
	 * Return "failure" for an unsuccessful database operation
	 */
	public String addUser(Login login) {
		System.out.println(login.getUsername());
		System.out.println(login.getPassword());
		System.out.println(login.getRole());
	    String sql = "INSERT INTO login (username, password, role) VALUES (?, ?, ?)";

	    try (Connection conn = DatabaseConnection.getConnection();
	         PreparedStatement ps = conn.prepareStatement(sql)) {

	        ps.setString(1, login.getUsername());
	        ps.setString(2, login.getPassword());
	        ps.setString(3, login.getRole());

	        int rows = ps.executeUpdate();
	        return rows > 0 ? "success" : "failure";

	    } catch (SQLException e) {
	        e.printStackTrace();
	        return "failure";
	    }
	}

}
