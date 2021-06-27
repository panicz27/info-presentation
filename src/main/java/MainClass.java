import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class MainClass {

    public static void main(String[] args) throws SQLException, FileNotFoundException, UnsupportedEncodingException {
        Connection conn = DbConnection.getConnection();
        DbConnection dbConnection = new DbConnection();


        String minimumKickOff = "00:00";
        String maximumKickOff = "23:59";
        String date = "2021-06-20";
        String endDate = "2021-06-20";

        boolean withVenue = true;
        PrintWriter writer = new PrintWriter("niedziela" + date + ".txt", "UTF-8");
        String[] stats = new String[]{"team_goals", "total_goals", "team_corners", "total_corners", "team_shots_on", "total_shots_on", "team_yellow_cards",
                "total_yellow_cards", "team_fouls", "total_fouls", "team_offsides", "total_offsides", "team_shots_off", "total_shots_off",
                "team_possession"};
        for (String stat : stats)
            dbConnection.displayStatsByDate("D", stat, date, endDate,
                    minimumKickOff, maximumKickOff, "6", !withVenue, conn, writer);

        for (String stat : stats)
            dbConnection.displayStatsByDate("D", stat, date, endDate,
                    minimumKickOff, maximumKickOff, "4", withVenue, conn, writer);

        writer.close();
    }


}
