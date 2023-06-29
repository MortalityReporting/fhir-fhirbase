package edu.gatech.chai.fhironfhirbase.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.stereotype.Component;

import edu.gatech.chai.fhironfhirbase.provider.PatientResourceProvider;
import edu.gatech.chai.fhironfhirbase.utilities.ExtensionUtil;

@Component
public class DatabaseConnectionPing {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledTask.class);

	@Autowired
	DatabaseConfiguration databaseConfiguration;

	@Scheduled(fixedDelay = 180000)
    public void pingDatabase () throws SQLException {
		Connection connection = null;
		// try {
		// 	connection = databaseConfiguration.getDataSource().getConnection();

		// 	String query = "SELECT count(*) as count FROM " + PatientResourceProvider.getType().toLowerCase() + ";";
		// 	logger.debug("Query to get size from ping: " + query);
		// 	PreparedStatement stmt = connection.prepareStatement(query);

		// 	ResultSet rs = stmt.executeQuery();
		// 	if (rs.next()) {
		// 		int count = rs.getInt("count");
        // 		ExtensionUtil.addResourceCount(PatientResourceProvider.getType(), (long) count);
		// 	}

		// 	connection.close();

		// } catch (SQLException e) {
		// 	if (connection != null) connection.close();
		// 	throw e;
		// }

    }
}
