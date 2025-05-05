package dao;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import model.Stock;

public class StockDao {
    public static void main(String[] args) {

    }

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

        String sql = "SELECT StockSymbol, StockName, StockType, SharePrice, NumShares, PriceDate FROM stock";

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
                stock.setDate(rs.getString("PriceDate")); // or rs.getDate(...).toString() if needed

                stocks.add(stock);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stocks;
    }

    /* TODO
	 * The students code to fetch data from the database will be written here
	 * Return stock matching symbol
	 */
    public Stock getStockBySymbol(String stockSymbol) {
        String sql = "SELECT StockSymbol, StockName, StockType, SharePrice, NumShares, PriceDate FROM stock WHERE StockSymbol = ?";
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
                stock.setDate(rs.getString("PriceDate")); // or convert to Date if needed
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return stock;
    }

    /* TODO
     * The students code to fetch data from the database will be written here
     * Perform price update of the stock symbol
     */
    public String setStockPrice(String stockSymbol, double stockPrice) {

        return "success";
    }

	/* TODO
	 * The students code to fetch data from the database will be written here
	 * Get list of bestseller stocks
	 */
	public List<Stock> getOverallBestsellers() {

		return getDummyStocks();

	}

	/* TODO
	 * The students code to fetch data from the database will be written here.
	 * Get list of customer bestseller stocks
	 */
    public List<Stock> getCustomerBestsellers(String customerID) {

        return getDummyStocks();

    }
    
	/* TODO
	 * The students code to fetch data from the database will be written here
	 * Get stockHoldings of customer with customerId
	 */
    public List<Stock> getStocksByCustomer(String customerId) {
        List<Stock> stocks = new ArrayList<>();

        String sql = "SELECT s.StockSymbol, s.StockName, s.StockType, s.SharePrice, s.NumShares, s.PriceDate, " +
                     "b.Quantity " +
                     "FROM stock s " +
                     "JOIN buy b ON s.StockSymbol = b.StockSymbol " +
                     "WHERE b.CustomerID = ?";

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
                stock.setNumShares(rs.getInt("Quantity")); // Quantity customer owns
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
	 * Return list of stocks matching "name"
	 */
    public List<Stock> getStocksByName(String name) {

        return getDummyStocks();
    }
    
	/* TODO
	 * The students code to fetch data from the database will be written here
	 * Return stock suggestions for given "customerId"
	 */
    public List<Stock> getStockSuggestions(String customerID) {

        return getDummyStocks();

    }

	/* TODO
	 * The students code to fetch data from the database
	 * Return list of stock objects, showing price history
	 */
    public List<Stock> getStockPriceHistory(String stockSymbol) {

        return getDummyStocks();
    }

	/* TODO
	 * The students code to fetch data from the database will be written here.
	 * Populate types with stock types
	 */
    public List<String> getStockTypes() {

        List<String> types = new ArrayList<String>();
        types.add("technology");
        types.add("finance");
        return types;

    }
    
	/* TODO
	 * The students code to fetch data from the database will be written here
	 * Return list of stocks of type "stockType"
	 */
    public List<Stock> getStockByType(String stockType) {

        return getDummyStocks();
    }
}
