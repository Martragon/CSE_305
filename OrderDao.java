package dao;

import model.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import dao.DatabaseConnection;

public class OrderDao {

    public Order getDummyTrailingStopOrder() {
        TrailingStopOrder order = new TrailingStopOrder();

        order.setId(1);
        order.setDatetime(new Date());
        order.setNumShares(5);
        order.setPercentage(12.0);
        return order;
    }

    public Order getDummyMarketOrder() {
        MarketOrder order = new MarketOrder();

        order.setId(1);
        order.setDatetime(new Date());
        order.setNumShares(5);
        order.setBuySellType("buy");
        return order;
    }

    public Order getDummyMarketOnCloseOrder() {
        MarketOnCloseOrder order = new MarketOnCloseOrder();

        order.setId(1);
        order.setDatetime(new Date());
        order.setNumShares(5);
        order.setBuySellType("buy");
        return order;
    }

    public Order getDummyHiddenStopOrder() {
        HiddenStopOrder order = new HiddenStopOrder();

        order.setId(1);
        order.setDatetime(new Date());
        order.setNumShares(5);
        order.setPricePerShare(145.0);
        return order;
    }

    public List<Order> getDummyOrders() {
        List<Order> orders = new ArrayList<Order>();

        for (int i = 0; i < 3; i++) {
            orders.add(getDummyTrailingStopOrder());
        }

        for (int i = 0; i < 3; i++) {
            orders.add(getDummyMarketOrder());
        }

        for (int i = 0; i < 3; i++) {
            orders.add(getDummyMarketOnCloseOrder());
        }

        for (int i = 0; i < 3; i++) {
            orders.add(getDummyHiddenStopOrder());
        }

        return orders;
    }

    public String submitOrder(Order order, Customer customer, Employee employee, Stock stock) {

		/*
		 * Student code to place stock order
		 * Employee can be null, when the order is placed directly by Customer
         * */
    	
    	String sqlAddOrder = "INSERT INTO Orders (OrderType, NumShares, Stop, DatePlaced, PriceType) VALUES (?, ?, ?, ?, ?)";
    	String sqlAddTrade = "INSERT INTO Trade (OrderID, AccountID, BrokerID, StockSymbol) VALUES (?, ?, ?, ?)";
    	
    	boolean is_hidden_or_trailing_stop = false;
    	String buy_or_sell = null;
    	double stop = 0;
    	int orderID = -1;
    	String price_type = null;
    	
    	// Get the information from the order object
    	if (order instanceof HiddenStopOrder) {
    		// type cast to get the pricepershare
    		HiddenStopOrder hso = (HiddenStopOrder) order;
    		is_hidden_or_trailing_stop = true;
    		buy_or_sell = "Sell";
    		stop = hso.getPricePerShare();
    		price_type = "HiddenStop";
    		
    	} else if (order instanceof TrailingStopOrder) {
    		// type cast to get the percentage
    		TrailingStopOrder tso = (TrailingStopOrder) order;
    		is_hidden_or_trailing_stop = true;
    		buy_or_sell = "Sell";
    		stop = tso.getPercentage();
    		price_type = "TrailingStop";
    		
    	} else if (order instanceof MarketOnCloseOrder) {
    		MarketOnCloseOrder moco = (MarketOnCloseOrder) order;
    		buy_or_sell = moco.getBuySellType();
    		price_type = "MarketOnClose";
    		
    	} else { // Market
    		MarketOrder mo = (MarketOrder) order;
    		buy_or_sell = mo.getBuySellType();
    		price_type = "Market";
    		
    	}
    	
    	// Connect to the database
    	try (Connection conn = DatabaseConnection.getConnection()) {
    		
    		// Don't commit anything yet before it's ready to send out.
            conn.setAutoCommit(false);
            
            try (PreparedStatement psOrder = conn.prepareStatement(sqlAddOrder, Statement.RETURN_GENERATED_KEYS)) {
            	psOrder.setString(1, buy_or_sell);
            	psOrder.setInt(2, order.getNumShares());
            	
            	// Set the Stop parameter if it is hidden or trailing, null otherwise
            	if (is_hidden_or_trailing_stop) {
            		psOrder.setDouble(3, stop);
            	} else {
            		psOrder.setNull(3, Types.DOUBLE);
            	}
            	
            	// Have to convert the util.java.Date to  util.sql.Date
            	psOrder.setDate(4, new java.sql.Date(order.getDatetime().getTime()));
            	
            	psOrder.setString(5, price_type);
            	
            	psOrder.executeUpdate();
            	
            	// Get the OrderID that was generated to connect it to the Trade table
            	try (ResultSet rs = psOrder.getGeneratedKeys()) {
            		if (rs.next()) {
            			orderID = rs.getInt(1);
            		} else {
            			throw new Exception("No ID generated");
            		}
            	}
            }
            
            try (PreparedStatement psTrade = conn.prepareStatement(sqlAddTrade)) {
            	psTrade.setInt(1, orderID);
            	psTrade.setInt(2, customer.getAccountNumber());
            	
            	// If the customer filled out an order themselves, employee is null
            	if (employee != null) {            		
            		psTrade.setInt(3, Integer.valueOf(employee.getEmployeeID()));
            	} else {
            		psTrade.setNull(3, Types.INTEGER);
            	}
            	
            	psTrade.setString(4, stock.getSymbol());
            	
            	psTrade.executeUpdate();
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

    public List<Order> getOrderByStockSymbol(String stockSymbol) {
        /*
		 * Student code to get orders by stock symbol
         */
    	
    	List<Order> orders = new ArrayList<Order>();
    	String sql = "SELECT o.OrderID, o.OrderType, o.NumShares, o.Stop, o.DatePlaced, o.PriceType, t.StockSymbol FROM Orders o JOIN Trade t ON o.OrderID = t.OrderID WHERE t.StockSymbol = ?";
    	
    	// Connect to the database, prepare the statement and get the result set
        // Don't need to fill in ? for sql or have multiple different queries so we can do it in one try statement
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            	 
        	ps.setString(1, stockSymbol);
        	
            try (ResultSet rs = ps.executeQuery()) {
        	
	        	// while there are rows to be read, keep on creating the employee object and add it to the list
	            while (rs.next()) {
	            	Order order;
	            	
	            	String priceType = rs.getString("PriceType");
	            	
	            	if (priceType.equals("HiddenStop")) {
	            		order = new HiddenStopOrder();
	            		((HiddenStopOrder)order).setPricePerShare(rs.getDouble("Stop"));
	            	} else if (priceType.equals("TrailingStop")) {
	            		order = new TrailingStopOrder();
	            		((TrailingStopOrder)order).setPercentage(rs.getDouble("Stop"));
	            	} else if (priceType.equals("MarketOnClose")) {
	            		order = new MarketOnCloseOrder();
	            		((MarketOnCloseOrder)order).setBuySellType(rs.getString("OrderType"));
	            	} else {
	            		order = new MarketOrder();
	            		((MarketOrder)order).setBuySellType(rs.getString("OrderType"));
	            	}
	            	
	            	order.setDatetime(rs.getDate("DatePlaced"));
	            	order.setId(rs.getInt("OrderID"));
	            	order.setNumShares(rs.getInt("NumShares"));
	            	
	            	orders.add(order);
	            }
            }
	            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    	return orders;
    }

    public List<Order> getOrderByCustomerName(String customerName) {
         /*
		 * Student code to get orders by customer name
         */
    	List<Order> orders = new ArrayList<Order>();
    	String sql = "SELECT o.OrderID, o.OrderType, o.NumShares, o.Stop, o.DatePlaced, o.PriceType, t.StockSymbol "
    			+ "FROM Person p JOIN Customer c ON p.SSN = c.CustomerID"
    			+ "JOIN Account a ON c.CustomerID = AccountID"
    			+ "JOIN Trade t ON a.AccountID = t.AccountID"
    			+ "JOIN Orders o ON o.OrderID = t.OrderID "
    			+ "WHERE p.FirstName = ?";
    	
    	// Connect to the database, prepare the statement and get the result set
        // Don't need to fill in ? for sql or have multiple different queries so we can do it in one try statement
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            	 
        	ps.setString(1, customerName);
        	
            try (ResultSet rs = ps.executeQuery()) {
        	
	        	// while there are rows to be read, keep on creating the employee object and add it to the list
	            while (rs.next()) {
	            	Order order;
	            	
	            	String priceType = rs.getString("PriceType");
	            	
	            	if (priceType.equals("HiddenStop")) {
	            		order = new HiddenStopOrder();
	            		((HiddenStopOrder)order).setPricePerShare(rs.getDouble("Stop"));
	            	} else if (priceType.equals("TrailingStop")) {
	            		order = new TrailingStopOrder();
	            		((TrailingStopOrder)order).setPercentage(rs.getDouble("Stop"));
	            	} else if (priceType.equals("MarketOnClose")) {
	            		order = new MarketOnCloseOrder();
	            		((MarketOnCloseOrder)order).setBuySellType(rs.getString("OrderType"));
	            	} else {
	            		order = new MarketOrder();
	            		((MarketOrder)order).setBuySellType(rs.getString("OrderType"));
	            	}
	            	
	            	order.setDatetime(rs.getDate("DatePlaced"));
	            	order.setId(rs.getInt("OrderID"));
	            	order.setNumShares(rs.getInt("NumShares"));
	            	
	            	orders.add(order);
	            }
            }
	            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    	return orders;
    }

    public List<Order> getOrderHistory(String customerId) {
        /*
		 * The students code to fetch data from the database will be written here
		 * Show orders for given customerId
		 */
    	List<Order> orders = new ArrayList<Order>();
    	String sql = "SELECT o.OrderID, o.OrderType, o.NumShares, o.Stop, o.DatePlaced, o.PriceType, t.StockSymbol "
    			+ "FROM Customer c JOIN Account a ON c.CustomerID = AccountID"
    			+ "JOIN Trade t ON a.AccountID = t.AccountID"
    			+ "JOIN Orders o ON o.OrderID = t.OrderID "
    			+ "WHERE c.CustomerID = ?";
    	
    	// Connect to the database, prepare the statement and get the result set
        // Don't need to fill in ? for sql or have multiple different queries so we can do it in one try statement
        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            	 
        	ps.setString(1, customerId);
        	
            try (ResultSet rs = ps.executeQuery()) {
        	
	        	// while there are rows to be read, keep on creating the employee object and add it to the list
	            while (rs.next()) {
	            	Order order;
	            	
	            	String priceType = rs.getString("PriceType");
	            	
	            	if (priceType.equals("HiddenStop")) {
	            		order = new HiddenStopOrder();
	            		((HiddenStopOrder)order).setPricePerShare(rs.getDouble("Stop"));
	            	} else if (priceType.equals("TrailingStop")) {
	            		order = new TrailingStopOrder();
	            		((TrailingStopOrder)order).setPercentage(rs.getDouble("Stop"));
	            	} else if (priceType.equals("MarketOnClose")) {
	            		order = new MarketOnCloseOrder();
	            		((MarketOnCloseOrder)order).setBuySellType(rs.getString("OrderType"));
	            	} else {
	            		order = new MarketOrder();
	            		((MarketOrder)order).setBuySellType(rs.getString("OrderType"));
	            	}
	            	
	            	order.setDatetime(rs.getDate("DatePlaced"));
	            	order.setId(rs.getInt("OrderID"));
	            	order.setNumShares(rs.getInt("NumShares"));
	            	
	            	orders.add(order);
	            }
            }
	            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    	return orders;
    }


    public List<OrderPriceEntry> getOrderPriceHistory(String orderId) {

        /*
		 * The students code to fetch data from the database will be written here
		 * Query to view price history of hidden stop order or trailing stop order
		 * Use setPrice to show hidden-stop price and trailing-stop price
		 */
        List<OrderPriceEntry> orderPriceHistory = new ArrayList<OrderPriceEntry>();

        for (int i = 0; i < 10; i++) {
            OrderPriceEntry entry = new OrderPriceEntry();
            entry.setOrderId(orderId);
            entry.setDate(new Date());
            entry.setStockSymbol("aapl");
            entry.setPricePerShare(150.0);
            entry.setPrice(100.0);
            orderPriceHistory.add(entry);
        }
        return orderPriceHistory;
    }
}
