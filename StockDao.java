package dao;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import model.Stock;

public class StockDao {
    

    public Stock getDummyStock() {
        Stock stock = new Stock();
        stock.setName("Apple");
        stock.setSymbol("AAPL");
        stock.setPrice(150.0);
        stock.setNumShares(1200);
        stock.setType("Technology");

        return stock;
    }

    public List<Stock> getDummyStocks() {
        List<Stock> stocks = new ArrayList<Stock>();

		/*Sample data begins*/
        for (int i = 0; i < 10; i++) {
            stocks.add(getDummyStock());
        }
		/*Sample data ends*/

        return stocks;
    }

	/* TODO
	 * The students code to fetch data from the database will be written here
	 * Query to fetch details of all the stocks has to be implemented
	 * Return list of actively traded stocks
	 * go into transactions, spit the most recently traded
	 */
    public List<Stock> getActivelyTradedStocks() {

        return getDummyStocks();

    }

	/*
	 * The students code to fetch data from the database will be written here
	 * Return list of stocks
	 */
    public List<Stock> getAllStocks() {
        List<Stock> stocks = new ArrayList<>();
        
        String sql = "SELECT s.StockSymbol, s.StockName, s.StockType, s.SharePrice, s.NumShares, s.PriceDate "
        		+ "FROM stock s "
        		+ "JOIN ( "
        		+ "    SELECT StockSymbol, MAX(PriceDate) AS LatestDate "
        		+ "    FROM stock "
        		+ "    GROUP BY StockSymbol "
        		+ ") latest "
        		+ "ON s.StockSymbol = latest.StockSymbol AND s.PriceDate = latest.LatestDate;";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Stock stock = new Stock();

                stock.setSymbol(rs.getString("StockSymbol"));
                stock.setName(rs.getString("StockName"));
                stock.setType(rs.getString("StockType"));
                stock.setPrice(rs.getDouble("SharePrice"));
                stock.setNumShares(rs.getInt("NumShares"));
                stock.setDate(rs.getString("PriceDate"));

                stocks.add(stock);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stocks;
    }

    /*
	 * The students code to fetch data from the database will be written here
	 * Return stock matching symbol
	 */
    public Stock getStockBySymbol(String stockSymbol) {
        String sql = "SELECT StockSymbol, StockName, StockType, SharePrice, NumShares, PriceDate " +
                     "FROM stock WHERE StockSymbol = ? " +
                     "ORDER BY PriceDate DESC LIMIT 1";
        
        Stock stock = null;

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, stockSymbol);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                stock = new Stock();
                stock.setSymbol(rs.getString("StockSymbol"));
                stock.setName(rs.getString("StockName"));
                stock.setType(rs.getString("StockType"));
                stock.setPrice(rs.getDouble("SharePrice"));
                stock.setNumShares(rs.getInt("NumShares"));
                stock.setDate(rs.getString("PriceDate")); // or use java.util.Date if desired
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stock;
    }


    public static void main(String[] args) {
    	StockDao s = new StockDao();
    	// System.out.println(s.setStockPrice("AAPL", 162));
    }
    
    /* 
     * The students code to fetch data from the database will be written here
     * Perform price update of the stock symbol
     * for the most recent date
     */
    public String setStockPrice(String stockSymbol, double stockPrice) {
        String sql = "INSERT INTO stock (StockSymbol, StockName, StockType, SharePrice, NumShares, PriceDate) " +
                     "SELECT StockSymbol, StockName, StockType, ?, NumShares, CURRENT_TIMESTAMP() " +
                     "FROM stock WHERE StockSymbol = ? ORDER BY PriceDate DESC LIMIT 1";

        try (Connection conn = DatabaseConnection.getConnection()) {
        	
        	// Don't commit anything yet
        	conn.setAutoCommit(false);
        	
        	// Get the old price to compare for trailing and hidden stops
        	double oldPrice = getLatestPrice(stockSymbol, conn);
        	
        	// System.out.println("old price: " + oldPrice);
        	
        	try (PreparedStatement ps = conn.prepareStatement(sql)) {

        		ps.setDouble(1, stockPrice);       // New price
        		ps.setString(2, stockSymbol);      // Symbol to copy from (latest record)
        	
        		int rowsInserted = ps.executeUpdate();
            	if (rowsInserted == 0) throw new SQLException ("No stock record to copy for symbol: " + stockSymbol);
        	}
        	
        	conn.commit();
        	
        	// If the stock price increases, update the hidden stops and trailing stops for their history
        	if (stockPrice > oldPrice) {
        		// System.out.println("1");
        		updateHiddenStops(stockSymbol, stockPrice, conn);
        		updateTrailingStops(stockSymbol, stockPrice, conn);
        		
        	// If the stock price ever lowers this is when it triggers
        	} else if (stockPrice < oldPrice) {
        		// System.out.println("2");
        		OrderDao d = new OrderDao();
        		d.processStopOrders();
        	}
        	
        	// commit changes
        	conn.commit();
        	return "success";
        } catch (SQLException e) {
            e.printStackTrace();
            return "failure";
        }
    }
    
    private double getLatestPrice(String symbol, Connection conn) throws SQLException {
    	String sql = "SELECT SharePrice FROM Stock WHERE StockSymbol = ? ORDER BY PriceDate DESC LIMIT 1";
    	try (PreparedStatement ps = conn.prepareStatement(sql)) {
    		ps.setString(1, symbol);
    		try (ResultSet rs = ps.executeQuery()) {
    			if (rs.next()) return rs.getDouble("SharePrice");
    		}
    	}
    	
    	throw new SQLException("No price found for symbol: " + symbol);
    }
    
    private void updateHiddenStops(String symbol, double newPrice, Connection conn) throws SQLException {
    	// gets the order id and stop from the order
    	String sqlGetOrders = "SELECT t.OrderID, o.Stop FROM Orders o JOIN Trade t ON o.OrderID = t.OrderID "
    			+ "WHERE o.PriceType = 'HiddenStop' AND t.StockSymbol = ? AND t.TransactionID IS NULL";
    	
    	
    	String sqlAddHistory = "INSERT INTO OrderHistory (OrderID, PricePerShare, Stop, DateTime) VALUES (?, ?, ?, CURRENT_TIMESTAMP())";
    	
    	try (PreparedStatement psOrders = conn.prepareStatement(sqlGetOrders)) {
    		psOrders.setString(1, symbol);
    		
    		// Excecute the first query
    		try (ResultSet rs = psOrders.executeQuery()) {
    			
    			// Prepare the second query
    			try(PreparedStatement psHistory = conn.prepareStatement(sqlAddHistory)) {
    				
    				// Getting t.OrderID and o.Stop from all orders that are HiddenStops and match the symbol and has not transacted
    				while (rs.next()) {
    					psHistory.setInt(1, rs.getInt("OrderID"));
    					psHistory.setDouble(2, newPrice);
    					psHistory.setDouble(3, rs.getDouble("Stop"));
    					
    					// Insert a new row containing the history.
    					psHistory.executeUpdate();
    				}
    			}
    		}
    	}
    }
    
    private void updateTrailingStops(String symbol, double newPrice, Connection conn) throws SQLException {
    	// gets the order id and stop from the order
    	String sqlGetOrders = "SELECT t.OrderID, o.Stop, o.Percentage FROM Orders o JOIN Trade t ON  o.OrderID = t.OrderID "
    			+ "WHERE o.PriceType = 'TrailingStop' AND t.StockSymbol = ? AND t.TransactionID IS NULL";
    	
    	// updates the stop value of a given order
    	String sqlUpdateOrder = "UPDATE Orders SET Stop = ? WHERE ORDERID = ?";
    	
    	String sqlAddHistory = "INSERT INTO OrderHistory (OrderID, PricePerShare, Stop, DateTime) VALUES (?, ?, ?, CURRENT_TIMESTAMP())";
    	
    	try (PreparedStatement psOrders = conn.prepareStatement(sqlGetOrders)) {
    		psOrders.setString(1, symbol);
    		
    		// Excecute the first query
    		try (ResultSet rs = psOrders.executeQuery()) {
    			
    			// Prepare the second and third query
    			try(PreparedStatement psUpdateOrders = conn.prepareStatement(sqlUpdateOrder);
					PreparedStatement psHistory = conn.prepareStatement(sqlAddHistory)) {
    				
    				// Getting t.OrderID and o.Stop from all orders that are HiddenStops and match the symbol and has not transacted
    				while (rs.next()) {
    					
    					int orderID = rs.getInt("OrderID");
    					double percentage = rs.getDouble("Percentage");
    					double newStop = newPrice * (1.0 - (percentage/100.0));
    					
    					// Update the Orders.Stop
    					psUpdateOrders.setDouble(1, newStop);
    					psUpdateOrders.setInt(2, orderID);
    					psUpdateOrders.executeUpdate();
    					
    					// Record the history
    					psHistory.setInt(1, orderID);
    					psHistory.setDouble(2, newPrice);
    					psHistory.setDouble(3, newStop);
    					
    					// Insert a new row containing the history.
    					psHistory.executeUpdate();
    				}
    			}
    		}
    	}
    	
    	
    }


	/* TODO
	 * The students code to fetch data from the database will be written here
	 * Get list of bestseller stocks
	 * which orders buys the most, but also counting employee
	 */
	public List<Stock> getOverallBestsellers() {

		return getDummyStocks();

	}

	/* TODO
	 * The students code to fetch data from the database will be written here.
	 * Get list of customer bestseller stocks
	 * which orders buy the most, doesn't matter if transaction is null, just customer
	 */
    public List<Stock> getCustomerBestsellers(String customerID) {

        return getDummyStocks();

    }
    
	/*
	 * The students code to fetch data from the database will be written here
	 * Get stockHoldings of customer with customerId
	 */
    public List<Stock> getStocksByCustomer(String customerId) {
        List<Stock> stocks = new ArrayList<>();

        String sql = 
            "SELECT s.StockSymbol, s.StockName, s.StockType, s.SharePrice, s.PriceDate, sp.NumShares " +
            "FROM account a " +
            "JOIN stockporfolio sp ON a.AccountID = sp.AccountID " +
            "JOIN stock s ON sp.StockSymbol = s.StockSymbol " +
            "WHERE a.CustomerID = ? AND s.PriceDate = ( " +
            "    SELECT MAX(s2.PriceDate) " +
            "    FROM stock s2 " +
            "    WHERE s2.StockSymbol = s.StockSymbol " +
            ")";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, customerId);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Stock stock = new Stock();
                stock.setSymbol(rs.getString("StockSymbol"));
                stock.setName(rs.getString("StockName"));
                stock.setType(rs.getString("StockType"));
                stock.setPrice(rs.getDouble("SharePrice"));
                stock.setNumShares(rs.getInt("NumShares"));
                stock.setDate(rs.getString("PriceDate"));

                stocks.add(stock);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stocks;
    }



	/*
	 * The students code to fetch data from the database will be written here
	 * Return list of stocks matching "name"
	 */
    public List<Stock> getStocksByName(String name) {
        List<Stock> stocks = new ArrayList<>();

        String sql = 
            "SELECT s1.StockSymbol, s1.StockName, s1.StockType, s1.SharePrice, s1.NumShares, s1.PriceDate " +
            "FROM stock s1 " +
            "JOIN ( " +
            "   SELECT StockSymbol, MAX(PriceDate) AS MaxDate " +
            "   FROM stock " +
            "   GROUP BY StockSymbol " +
            ") s2 ON s1.StockSymbol = s2.StockSymbol AND s1.PriceDate = s2.MaxDate " +
            "WHERE s1.StockName LIKE ?";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, "%" + name + "%");
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Stock stock = new Stock();
                stock.setSymbol(rs.getString("StockSymbol"));
                stock.setName(rs.getString("StockName"));
                stock.setType(rs.getString("StockType"));
                stock.setPrice(rs.getDouble("SharePrice"));
                stock.setNumShares(rs.getInt("NumShares"));
                stock.setDate(rs.getString("PriceDate"));
                stocks.add(stock);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stocks;
    }

	/* TODO
	 * The students code to fetch data from the database will be written here
	 * Return stock suggestions for given "customerId"
	 * SIMILAR STOCK TYPES
	 */
    public List<Stock> getStockSuggestions(String customerID) {
        List<Stock> suggestions = new ArrayList<>();

        String sql = 
            "SELECT s.StockSymbol, s.StockName, s.StockType, s.SharePrice, s.NumShares, s.PriceDate "
            + "FROM Stock AS s JOIN ("
            + "SELECT DISTINCT  st.StockType "
            + "FROM Stock AS st JOIN StockPorfolio AS sp ON sp.StockSymbol = st.StockSymbol "
            + "JOIN Account AS a ON a.AccountID = sp.AccountID "
            + "WHERE a.CustomerID = ?"
            + ") AS custTypes ON s.StockType = custTypes.StockType JOIN ("
            + " SELECT StockSymbol,  MAX(PriceDate) AS LatestDate FROM Stock GROUP BY StockSymbol"
            + ") AS latest ON s.StockSymbol = latest.StockSymbol AND s.PriceDate = latest.LatestDate "
            + "ORDER BY s.StockType, s.StockSymbol";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, customerID);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Stock stock = new Stock();
                stock.setSymbol(rs.getString("StockSymbol"));
                stock.setName(rs.getString("StockName"));
                stock.setType(rs.getString("StockType"));
                stock.setPrice(rs.getDouble("SharePrice"));
                stock.setNumShares(rs.getInt("NumShares"));
                stock.setDate(rs.getString("PriceDate"));
                suggestions.add(stock);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return suggestions;
    }


	/*
	 * The students code to fetch data from the database
	 * Return list of stock objects, showing price history
	 */
    public List<Stock> getStockPriceHistory(String stockSymbol) {
        List<Stock> priceHistory = new ArrayList<>();

        String sql = "SELECT StockSymbol, StockName, StockType, SharePrice, NumShares, PriceDate " +
                     "FROM stock " +
                     "WHERE StockSymbol = ? " +
                     "ORDER BY PriceDate ASC";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, stockSymbol);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Stock stock = new Stock();
                stock.setSymbol(rs.getString("StockSymbol"));
                stock.setName(rs.getString("StockName"));
                stock.setType(rs.getString("StockType"));
                stock.setPrice(rs.getDouble("SharePrice"));
                stock.setNumShares(rs.getInt("NumShares"));
                stock.setDate(rs.getString("PriceDate")); // or convert to Date if needed
                priceHistory.add(stock);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return priceHistory;
    }


	/*
	 * The students code to fetch data from the database will be written here.
	 * Populate types with stock types
	 */
    public List<String> getStockTypes() {
        List<String> types = new ArrayList<>();

        String sql = "SELECT DISTINCT StockType FROM stock";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                types.add(rs.getString("StockType"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return types;
    }

    
	/*
	 * The students code to fetch data from the database will be written here
	 * Return list of stocks of type "stockType"
	 */
    public List<Stock> getStockByType(String stockType) {
        List<Stock> stocks = new ArrayList<>();

        String sql = "SELECT StockSymbol, StockName, StockType, SharePrice, NumShares, PriceDate " +
                     "FROM stock WHERE StockType = ? " +
                     "AND (StockSymbol, PriceDate) IN ( " +
                     "    SELECT StockSymbol, MAX(PriceDate) " +
                     "    FROM stock " +
                     "    GROUP BY StockSymbol " +
                     ")";

        try (Connection conn = DatabaseConnection.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, stockType);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                Stock stock = new Stock();
                stock.setSymbol(rs.getString("StockSymbol"));
                stock.setName(rs.getString("StockName"));
                stock.setType(rs.getString("StockType"));
                stock.setPrice(rs.getDouble("SharePrice"));
                stock.setNumShares(rs.getInt("NumShares"));
                stock.setDate(rs.getString("PriceDate")); // Optional: convert to Date if needed
                stocks.add(stock);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stocks;
    }

}
