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
    	
    	String sqlAddOrder = "INSERT INTO Orders (OrderType, NumShares, Stop, Percentage, DatePlaced, PriceType) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(), ?)";
    	String sqlAddTrade = "INSERT INTO Trade (OrderID, AccountID, BrokerID, StockSymbol) VALUES (?, ?, ?, ?)";
    	
    	boolean is_hidden_or_trailing_stop = false;
    	String buy_or_sell;
    	double stop = 0;
    	double percentage = 0;
    	int orderID;
    	String price_type;
    	int shares = order.getNumShares();
    	
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
    		stop = stock.getPrice() * (1.0 - (tso.getPercentage() / 100.0));
    		percentage = tso.getPercentage();
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
            
            // Insert into Orders and get the OrderID
            try (PreparedStatement psOrder = conn.prepareStatement(sqlAddOrder, Statement.RETURN_GENERATED_KEYS)) {
            	psOrder.setString(1, buy_or_sell);
            	psOrder.setInt(2, order.getNumShares());
            	
            	// Set the Stop parameter if it is hidden or trailing, null otherwise
            	if (is_hidden_or_trailing_stop) {
            		psOrder.setDouble(3, stop);
            		psOrder.setDouble(4, percentage);
            	} else {
            		psOrder.setNull(3, Types.DOUBLE);
            		psOrder.setNull(4, Types.DOUBLE);
            	}
            	
            	psOrder.setString(5, price_type);
            	
            	psOrder.executeUpdate();
            	
            	// Get the OrderID that was generated to connect it to the Trade table
            	try (ResultSet rs = psOrder.getGeneratedKeys()) {
            		if (rs.next()) {
            			orderID = rs.getInt(1);
            		} else {
            			throw new Exception("No Order ID generated");
            		}
            	}
            }
            
            // Market and MarketOnClose orders execute immediately
            if (!is_hidden_or_trailing_stop) {
            	
            	// Check first if we're able to buy the stock from the inventory. If so, update the stock inventory
            	checkAndUpdateInventory(stock.getSymbol(), buy_or_sell, shares, conn);
            	
            	// Calculate the fee
            	double pricePerShare = stock.getPrice();
            	double totalValue = pricePerShare * shares;
            	double fee = totalValue * 0.05;
            	
            	int transactionID;
            	String sqlAddTransaction = "INSERT INTO `Transaction` (Fee, DateTime, PricePerShare) VALUES (?, CURRENT_TIMESTAMP(), ?)";
            	
            	// Insert into Transaction 
            	try (PreparedStatement psTransaction = conn.prepareStatement(sqlAddTransaction, Statement.RETURN_GENERATED_KEYS)) {
            		psTransaction.setDouble(1, fee);
            		psTransaction.setDouble(2, pricePerShare);
            		psTransaction.executeUpdate();
            		
            		// Get the Transaction ID to connect it to the Trade Table
            		try (ResultSet rs = psTransaction.getGeneratedKeys()) {
            			if (rs.next()) {
            				transactionID = rs.getInt(1);
            			} else {
            				throw new Exception("No Transaction ID Generated");
            			}
            		}
            	}
            	
            	String sqlAddTradeWithTransaction = "INSERT INTO Trade (OrderID, AccountID, BrokerID, StockSymbol, TransactionID) VALUES (?, ?, ?, ?, ?)";
            	try (PreparedStatement psTradeTransaction = conn.prepareStatement(sqlAddTradeWithTransaction)) {
                	psTradeTransaction.setInt(1, orderID);
                	psTradeTransaction.setInt(2, customer.getAccountNumber());
//                	psTrade.setInt(2, 1);
                	
                	// If the customer filled out an order themselves, employee is null
                	if (employee != null) {            		
                		psTradeTransaction.setInt(3, Integer.valueOf(employee.getEmployeeID()));
                	} else {
                		psTradeTransaction.setNull(3, Types.INTEGER);
                	}
                	
                	psTradeTransaction.setString(4, stock.getSymbol());
                	psTradeTransaction.setInt(5, transactionID);
                	
                	psTradeTransaction.executeUpdate();
                }
            	
            	// Add or Subtract from the portfolio
            	adjustPortfolio(customer.getAccountNumber(), stock.getSymbol(), buy_or_sell, shares, conn);
            	
        	} else { // This is a HiddenStop or Trailing Stop and won't be executed yet
        		try (PreparedStatement psTrade = conn.prepareStatement(sqlAddTrade)) {
                	psTrade.setInt(1, orderID);
                	psTrade.setInt(2, customer.getAccountNumber());
//                	psTrade.setInt(2, 1);
                	
                	// If the customer filled out an order themselves, employee is null
                	if (employee != null) {            		
                		psTrade.setInt(3, Integer.parseInt(employee.getEmployeeID()));
                	} else {
                		psTrade.setNull(3, Types.INTEGER);
                	}
                	
                	psTrade.setString(4, stock.getSymbol());
                	
                	psTrade.executeUpdate();
        		}
        		
        		String sqlAddHistory = "INSERT INTO OrderHistory (OrderID, PricePerShare, Stop, DateTime) VALUES (?, ?, ?, CURRENT_TIMESTAMP())";
            	
        		// Insert into the history of the order
    			try(PreparedStatement psHistory = conn.prepareStatement(sqlAddHistory)) {
					psHistory.setInt(1, orderID);
					psHistory.setDouble(2, stock.getPrice());
					psHistory.setDouble(3, stop);
					
					// Insert a new row containing the history.
					psHistory.executeUpdate();
    			}
        		
        		// Process the orders in case it was already triggered
        		processStopOrders();
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
    

    // Executes any pending HiddenStop and TrailingStop orders whose stop price or percentage has been triggered
    // All triggered trades incur a 5% fee and update inventory and portfolio.
    public void processStopOrders() {
    	// System.out.println("stop orders");
    	// Get the all the data where the order is a HiddenStop/Trailing Stop and there is no TransactionID in the trade
        String sqlFetch = "SELECT o.OrderID, o.NumShares, o.Stop, t.AccountID, t.BrokerID, t.StockSymbol, o.PriceType " +
                          "FROM Orders o JOIN Trade t ON o.OrderID = t.OrderID " +
                          "WHERE o.PriceType IN ('HiddenStop','TrailingStop') AND t.TransactionID IS NULL";
        
        try (Connection conn = DatabaseConnection.getConnection()) {
        	
        	// Don't do anything yet
            conn.setAutoCommit(false);
            
            // Go through with the query
            try (PreparedStatement ps = conn.prepareStatement(sqlFetch);
                 ResultSet rs = ps.executeQuery()) {
            	
            	// Queries through all of above
                while (rs.next()) {
                	// System.out.println("we got something");
                	
                    int orderId = rs.getInt("OrderID");
                    int shares = rs.getInt("NumShares");
                    double stopVal = rs.getDouble("Stop");
                    int accountId = rs.getInt("AccountID");
                    String symbol = rs.getString("StockSymbol");
                    String priceType = rs.getString("PriceType");
                    
                    // Get the current price of a given stock symbol
                    StockDao s = new StockDao();
                    Stock stock = s.getStockBySymbol(symbol);
                    double currentPrice = stock.getPrice();
                    
                    // trigger if the current price is less than or equal to the stopping value
                    boolean triggered = false;
                    
                    if ("HiddenStop".equals(priceType) && currentPrice <= stopVal) triggered = true;
                    else if ("TrailingStop".equals(priceType) && currentPrice <= stopVal) triggered = true;
                    
                    // System.out.println("triggered: " + triggered);
                    
                    if (triggered) {
                        // Update inventory (sell adds shares back)
                        checkAndUpdateInventory(symbol, "Sell", shares, conn);
                        // System.out.println("Inventory checked out");

                        double fee = currentPrice * shares * 0.05;
                        
                        int transactionID;
                        String sqlAddTransaction = "INSERT INTO `Transaction` (Fee, DateTime, PricePerShare) VALUES (?, CURRENT_TIMESTAMP(), ?)";
                        
                        // Make a new transaction
                        try (PreparedStatement psTransaction = conn.prepareStatement(sqlAddTransaction, Statement.RETURN_GENERATED_KEYS)) {
                            psTransaction.setDouble(1, fee);
                            psTransaction.setDouble(2, currentPrice);
                            
                            psTransaction.executeUpdate();
                            
                            // Get the transactionID to put into the Trade
                            try (ResultSet rk = psTransaction.getGeneratedKeys()) {
                                if (rk.next()) transactionID = rk.getInt(1);
                                else throw new SQLException("Failed to retrieve TransactionID");
                            }
                        }
                        
                        // System.out.println("Transaction checked out");
                        
                        // Update the Trade with the new Transaction
                        String sqlTradeUpdate = "UPDATE Trade SET TransactionID = ? WHERE OrderID = ?";
                        try (PreparedStatement psUpdate = conn.prepareStatement(sqlTradeUpdate)) {
                            psUpdate.setInt(1, transactionID);
                            psUpdate.setInt(2, orderId);
                            psUpdate.executeUpdate();
                        }
                        
                        // System.out.println("Trade checked out");
                        
                        // Update the Portfolio by amount sold
                        adjustPortfolio(accountId, symbol, "Sell", shares, conn);
                        
                        // System.out.println("Portfolio checked out");
                        
                        String sqlAddHistory = "INSERT INTO OrderHistory (OrderID, PricePerShare, Stop, DateTime) VALUES (?, ?, ?, CURRENT_TIMESTAMP())";
                    	
                		// Insert into the history of the order
            			try(PreparedStatement psHistory = conn.prepareStatement(sqlAddHistory)) {
        					psHistory.setInt(1, orderId);
        					psHistory.setDouble(2, stock.getPrice());
        					psHistory.setDouble(3, stopVal);
        					
        					// Insert a new row containing the history.
        					psHistory.executeUpdate();
            			}
            			
            			// System.out.println("History checked out");
                        
                    }
                }
                
                // Go through with all actions
                conn.commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args) {
    	OrderDao d = new OrderDao();
    	d.processStopOrders();
    }
    
    //Validates and updates stock inventory for a given symbol
    private void checkAndUpdateInventory(String symbol, String action, int shares, Connection conn) throws SQLException {
        // Get number of shares available shares on the most recent price-date
        String sqlGetShares = "SELECT NumShares, PriceDate FROM Stock WHERE StockSymbol = ? ORDER BY PriceDate DESC LIMIT 1";
        
        int available;
        Timestamp stock_date;
        
        try (PreparedStatement ps = conn.prepareStatement(sqlGetShares)) {
        	// WHERE StockSymbol = ?
            ps.setString(1, symbol);
            
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new SQLException("Stock symbol not found: " + symbol);
                available = rs.getInt("NumShares");
                stock_date = rs.getTimestamp("PriceDate");
            }
        }
        
        // Updated stock after Buying or Selling
        int updated = action.equalsIgnoreCase("Buy") ? available - shares : available + shares;
        
        // System.out.println("Updated: " + updated);
        
        // If there isn't enough stock, throw an exception
        if (updated < 0) throw new SQLException("Not enough stock available to buy: " + symbol);
        
        // System.out.println("symbol: " + symbol + "; stock_date: " + stock_date);

        // Update the stock row with the most recent Date
        String sqlUpdate = 
                "UPDATE Stock SET NumShares = ? WHERE StockSymbol = ? AND PriceDate = ?";
        try (PreparedStatement psUpdate = conn.prepareStatement(sqlUpdate)) {
        	psUpdate.setInt(1, updated);
        	psUpdate.setString(2, symbol);
        	psUpdate.setTimestamp(3, stock_date);
            psUpdate.executeUpdate();
        }
    }
    
    // Adjust portfolio holdings after a trade for a given account and stock symbol
    private void adjustPortfolio(int accountId, String symbol, String action, int shares, Connection conn) throws SQLException {
        String selectPortfolio = "SELECT NumShares FROM StockPorfolio WHERE AccountID = ? AND StockSymbol = ?";
        String updatePortfolio = "UPDATE StockPorfolio SET NumShares = ? WHERE AccountID = ? AND StockSymbol = ?";
        String insertPortfolio = "INSERT INTO StockPorfolio (AccountID, StockSymbol, NumShares) VALUES (?, ?, ?)";
        
        // Get the number of shares from the stock portfolio. Update if it's already there, Insert if not
        try (PreparedStatement ps = conn.prepareStatement(selectPortfolio)) {
        	
            ps.setInt(1, accountId);
            ps.setString(2, symbol);
            
            try (ResultSet rs = ps.executeQuery()) {
            	
            	// If we get a result (The stock is already in the portfolio)
                if (rs.next()) {
                    int current = rs.getInt("NumShares");
                    
                    // Updated stock after Buying or Selling
                    int updated = "Buy".equalsIgnoreCase(action) ? current + shares : current - shares;
                    
                    // If there isn't enough stock, throw an exception
                    if (updated < 0) throw new SQLException("Insufficient shares to sell");
                    
                    // Update the portfolio because we're able to
                    try (PreparedStatement psUp = conn.prepareStatement(updatePortfolio)) {
                        psUp.setInt(1, updated);
                        psUp.setInt(2, accountId);
                        psUp.setString(3, symbol);
                        psUp.executeUpdate();
                    }
                   
                // The stock is not in the protfolio
                } else {
                	
                	// If we're trying to buy
                    if ("Buy".equalsIgnoreCase(action)) {
                    	// Insert the number of stocks  to buy
                        try (PreparedStatement psIn = conn.prepareStatement(insertPortfolio)) {
                            psIn.setInt(1, accountId);
                            psIn.setString(2, symbol);
                            psIn.setInt(3, shares);
                            psIn.executeUpdate();
                        }
                        
                    } else { // If we're trying to sell a stock that the account doesn't have
                        throw new SQLException("No shares to sell for account: " + accountId + ", symbol: " + symbol);
                    }
                }
            }
        }
    }

    public List<Order> getOrderByStockSymbol(String stockSymbol) {
        /*
		 * Student code to get orders by stock symbol
         */
    	
    	List<Order> orders = new ArrayList<Order>();
    	String sql = "SELECT o.OrderID, o.OrderType, o.NumShares, o.Stop, o.Percentage, o.DatePlaced, o.PriceType, t.StockSymbol FROM Orders o JOIN Trade t ON o.OrderID = t.OrderID WHERE t.StockSymbol = ?";
    	
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
	            		((TrailingStopOrder)order).setPercentage(rs.getDouble("Percentage"));
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
    	String sql = "SELECT o.OrderID, o.OrderType, o.NumShares, o.Stop, o.Percentage, o.DatePlaced, o.PriceType, t.StockSymbol "
    			+ "FROM Person p JOIN Customer c ON p.SSN = c.CustomerID "
    			+ "JOIN Account a ON c.CustomerID = AccountID "
    			+ "JOIN Trade t ON a.AccountID = t.AccountID "
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
	            		((TrailingStopOrder)order).setPercentage(rs.getDouble("Percentage"));
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
    	String sql = "SELECT o.OrderID, o.OrderType, o.NumShares, o.Stop, o.Percentage, o.DatePlaced, o.PriceType "
    			+ "FROM Customer c JOIN Account a ON c.CustomerID = a.CustomerID "
    			+ "JOIN Trade t ON a.AccountID = t.AccountID "
    			+ "JOIN Orders o ON o.OrderID = t.OrderID "
    			+ "WHERE c.CustomerID = ? ";
    	
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
	            	
	            	switch (priceType) {
	                    case "HiddenStop":
	                        HiddenStopOrder hso = new HiddenStopOrder();
	                        hso.setPricePerShare(rs.getDouble("Stop"));
	                        order = hso;
	                        break;
	                    case "TrailingStop":
	                        TrailingStopOrder tso = new TrailingStopOrder();
	                        tso.setPercentage(rs.getDouble("Percentage"));
	                        order = tso;
	                        break;
	                    case "MarketOnClose":
	                        MarketOnCloseOrder moco = new MarketOnCloseOrder();
	                        moco.setBuySellType(rs.getString("OrderType"));
	                        order = moco;
	                        break;
	                    default:
	                        MarketOrder mo = new MarketOrder();
	                        mo.setBuySellType(rs.getString("OrderType"));
	                        order = mo;
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
