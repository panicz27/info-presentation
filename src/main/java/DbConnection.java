import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;

import java.util.HashMap;
import java.util.Map;

public class DbConnection {

    private static final String QUERY_BY_DATE_ALL = "  SELECT TO_CHAR(MATCH_DATE,'DD-MM hh24:mi') AS GODZINA, HOME_TEAM AS GOSPODARZ," +
            " AWAY_TEAM AS GOSC, e1.TEAM_CORNERS AS GOSPODARZ_SREDNIA, e2.TEAM_CORNERS AS GOSC_SREDNIA,(e1.TEAM_CORNERS+e2.TEAM_CORNERS)/2 AS SREDNIA, home_number as MECZE_GOSPODARZ, away_number AS MECZE_GOSC" +
            " FROM MATCHES m JOIN (\n" +
            "  select avg(%s) as team_corners, team, count(team) as home_number from(\n" +
            "  with rws as (\n" +
            "  select o.*, row_number() over(\n" +
            "  partition by team\n" +
            "  order by event_id desc)\n" +
            "  rn\n" +
            "  from team_stats o\n" +
            "  ) select * from rws\n" +
            "  where rn<=?)\n" +
            "  where ((team in (\n" +
            "  select home_team from matches where to_char(match_date,'yyyy-MM-dd')>=? AND TO_CHAR(MATCH_DATE,'yyyy-MM-dd')<=? )))\n" +
            "  group by team\n" +
            "  order by team_corners desc) e1 ON e1.TEAM=m.HOME_TEAM\n" +
            "  JOIN( select avg(%s) as team_corners, team,count(team) as away_number from(\n" +
            "  with rws as (\n" +
            "  select o.*, row_number() over(\n" +
            "  partition by team\n" +
            "  order by event_id desc)\n" +
            "  rn\n" +
            "  from team_stats o\n" +
            "  ) select * from rws\n" +
            "  where rn<=?)\n" +
            "  where ((team in (\n" +
            "  select away_team from matches where to_char(match_date,'yyyy-MM-dd')>=? AND TO_CHAR(MATCH_DATE,'yyyy-MM-dd')<=? ) ))\n" +
            "  group by team\n" +
            "  order by team_corners desc) e2 ON e2.TEAM=m.AWAY_TEAM\n" +
            "  WHERE TO_CHAR(MATCH_DATE,'yyyy-MM-dd')>=? AND TO_CHAR(MATCH_DATE,'yyyy-MM-dd')<=?  AND  TO_CHAR(MATCH_DATE,'HH24:mi')<=? %s\n" +
            "  oRDER BY (e1.TEAM_CORNERS+e2.TEAM_CORNERS)/2 %s";

    private static final String QUERY_BY_DATE_VENUE = " SELECT TO_CHAR(MATCH_DATE,'DD-MM hh24:mi') AS GODZINA, HOME_TEAM AS GOSPODARZ," +
            "AWAY_TEAM AS GOSC, e1.TEAM_CORNERS AS GOSPODARZ_SREDNIA, e2.TEAM_CORNERS AS GOSC_SREDNIA,(e1.TEAM_CORNERS+e2.TEAM_CORNERS)/2 AS SREDNIA, home_number as MECZE_GOSPODARZ, away_number AS MECZE_GOSC" +
            " FROM MATCHES m JOIN (\n" +
            "  select avg(%s) as team_corners, team , count(team) as home_number from(\n" +
            "  with rws as (\n" +
            "  select o.*, row_number() over(\n" +
            "  partition by team\n" +
            "  order by event_id desc)\n" +
            "  rn\n" +
            "  from team_stats o where o.venue='H'\n" +
            "  ) select * from rws\n" +
            "  where rn<=?)\n" +
            "  where ((team in (\n" +
            "  select home_team from matches where to_char(match_date,'yyyy-MM-dd')>=? AND TO_CHAR(MATCH_DATE,'yyyy-MM-dd')<=?   )))\n" +
            "  group by team\n" +
            "  order by team_corners desc) e1 ON e1.TEAM=m.HOME_TEAM\n" +
            "  JOIN( select avg(%s) as team_corners, team, count(team) as away_number from(\n" +
            "  with rws as (\n" +
            "  select o.*, row_number() over(\n" +
            "  partition by team\n" +
            "  order by event_id desc)\n" +
            "  rn\n" +
            "  from team_stats o where o.venue='A'\n" +
            "  ) select * from rws\n" +
            "  where rn<=?)\n" +
            "  where ((team in (\n" +
            "  select away_team from matches where to_char(match_date,'yyyy-MM-dd')>=? AND TO_CHAR(MATCH_DATE,'yyyy-MM-dd')<=? )  ))\n" +
            "  group by team\n" +
            "  order by team_corners desc) e2 ON e2.TEAM=m.AWAY_TEAM\n" +
            "  WHERE TO_CHAR(MATCH_DATE,'yyyy-MM-dd')>=? AND TO_CHAR(MATCH_DATE,'yyyy-MM-dd')<=?   AND TO_CHAR(MATCH_DATE,'HH24:mi')<=? %s\n" +
            "  oRDER BY (e1.TEAM_CORNERS+e2.TEAM_CORNERS)/2 %s";

    public static Connection getConnection() throws SQLException {
        String url = "jdbc:oracle:thin:@localhost:1521:xe";
        String user = "system";
        String pass = "password";
        Connection con = null;
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        con = DriverManager.getConnection(url, user, pass);
        return con;
    }

    public void displayStatsByDate(String direction, String field, String date, String endDate, String minimumKickOff, String maximumKickOff, String howManyMatches, boolean withVenue, Connection conn, PrintWriter writer) throws SQLException {
        String temp = direction.equals("A") ? "ASC" : "DESC";

        String minimumKickOffString = null;
        if (minimumKickOff != null)
            minimumKickOffString = " AND TO_CHAR(MATCH_DATE,'HH24:mi')>='" + minimumKickOff + "'";
        else minimumKickOffString = " ";
        String query;
        if (withVenue)
            query = String.format(QUERY_BY_DATE_VENUE, field, field, minimumKickOffString, temp);
        else query = String.format(QUERY_BY_DATE_ALL, field, field, minimumKickOffString, temp);
        PreparedStatement prst = conn.prepareStatement(query);
        prst.setString(1, howManyMatches);
        prst.setString(2, date);
        prst.setString(3, endDate);
        prst.setString(4, howManyMatches);
        prst.setString(5, date);
        prst.setString(6, endDate);
        prst.setString(7, date);
        prst.setString(8, endDate);
        prst.setString(9, maximumKickOff);
        ResultSet rs = prst.executeQuery();
        ResultSetMetaData metadata = rs.getMetaData();
        String tempString = "                           ";
        String value = "";
        String row = "";
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            value = metadata.getColumnLabel(i);
            int length = tempString.length() - metadata.getColumnLabel(i).length();
            for (int j = 0; j < length; j++) {
                value += " ";
            }
            row += value;
        }
        Map<String, String> namesMapping = this.getMappingErrorNames();
        String checkVenue = withVenue ? "Z UWZGLEDNIENIEM MECZOW DOM/WYJAZD" : "BEZ UWZGLEDNENIA DOM/WYJAZD";
        writer.println("OSTATNIE " + howManyMatches + " MECZY, " + checkVenue + " statystyka: " + namesMapping.get(field));

        writer.println("");
        writer.println(row);
        tempString = "                           ";
        value = "";
        while (rs.next()) {
            row = "";
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                if (metadata.getColumnLabel(i).equals("GOSPODARZ_SREDNIA") || metadata.getColumnLabel(i).equals("GOSC_SREDNIA") ||
                        metadata.getColumnLabel(i).equals("SREDNIA")) {
                    BigDecimal bd = new BigDecimal(Double.valueOf(rs.getString(i)));
                    bd = bd.setScale(2, RoundingMode.HALF_UP);
                    value = bd.toString();

                } else
                    value = rs.getString(i);
                int length = tempString.length() - value.length();
                for (int j = 0; j < length; j++) {
                    value += " ";
                }
                row += value;
            }
            writer.println(row);
        }

        for (int i = 0; i < 4; i++)
            writer.println("");
    }

    public Map<String, String> getMappingErrorNames() {

        Map<String, String> mappingErrorNames = new HashMap() {{
            put("team_goals", "GOLE DRUZYNY");
            put("total_goals", "GOLE W MECZU");
            put("team_corners", "RZUTY ROŻNE DRUŻYNY");
            put("total_corners", "RZUTY ROŻNE W MECZU");
            put("team_shots_on", "STRZALY CELNE DRUZYNY");
            put("total_shots_on", "STRZALY CELNE W MECZU");
            put("team_yellow_cards", "ZOLTE KARTKI DRUZYNY");
            put("total_yellow_cards", "ZOLTE KARTKI MECZU");
            put("team_fouls", "FAULE DRUZYNY");
            put("total_fouls", "FAULE W MECZU");
            put("team_offsides", "SPALONE DRUZYNY");
            put("total_offsides", "SPALONE W MECZU");
            put("team_shots_off", "STRZALY NIECELNE DRUZYNY");
            put("total_shots_off", "STRZALY NIECELNE W MECZU");
            put("team_possession", "POSIADANIE DRUZYNY");

        }};
        return mappingErrorNames;
    }
}
