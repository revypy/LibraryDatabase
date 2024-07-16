import java.sql.*;

public class JDBCRunner {
    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "172.31.31.23/";
    private static final String DATABASE_NAME = "library";
    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";


    public static void main(String[] args) {
        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //All tables
            getAuthors(connection); System.out.println();
            getBooks(connection); System.out.println();
            getLoans(connection); System.out.println();
            getReaders(connection); System.out.println();

            //no param
            getAuthorsBooks(connection); System.out.println();
            getNoLoansBook(connection);System.out.println();
            getCopiesEachBook(connection);System.out.println();
            getCountAuthorsBooks(connection);System.out.println();
            getMonthBooks(connection);System.out.println();
            getReadersWithAnyBooks(connection);System.out.println();

            //param
            getBookByDate(connection, Date.valueOf("2000-10-10")); System.out.println();
            getInterestReadersWithThisBooks (connection, "Бойцовский клуб"); System.out.println();

            //correction
            addReader(connection, "Ivan", "Ivanov", "exemple@who.ru", "78984542519"); System.out.println();
            addBook(connection, "Бойцовский клуб", 2, Date.valueOf("1980-10-10"), 100);


        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }


    public static void checkDriver () {
        try {
            //Если класс найден, то JDBC драйвер загружен и готов к использованию
            Class.forName(DRIVER);

        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            //установка соединения с БД, используя предоставленные параметры.
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // Получить всю таблицу Authors
    private static void getAuthors(Connection connection) throws SQLException {
        String columnName0 = "author_id", columnName1 = "first_name", columnName2 = "last_name", columnName3 = "birth_date";
        int param0 = -1, param2 = -1;
        Date param3 = Date.valueOf("0000-00-00");
        String param1 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM authors;");

        while (rs.next()) {
            param3 = rs.getDate(columnName3);
            param2 = rs.getInt(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

    // Получить всю таблицу Books
    private static void getBooks(Connection connection) throws SQLException {
        String columnName0 = "book_id", columnName1 = "title", columnName2 = "author_id", columnName3 = "published_year", columnName4 = "copies_available";
        int param0 = -1, param2 = -1, param4 = -1;
        String param1 = null;
        Date param3 = Date.valueOf("0000-00-00");

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM books;");

        while (rs.next()) {
            param4 = rs.getInt(columnName4);
            param3 = rs.getDate(columnName3);
            param2 = rs.getInt(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }
    }

    // Получить всю таблицу Loans
    private static void getLoans(Connection connection) throws SQLException {
        String columnName0 = "loan_id", columnName1 = "book_id", columnName2 = "reader_id", columnName3 = "loan_date", columnName4 = "return_date";
        int param0 = -1, param1 = -1, param2 = -1;
        Date param3 = Date.valueOf("0000-00-00"), param4 = Date.valueOf("0000-00-00");

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM loans;");

        while (rs.next()) {
            param4 = rs.getDate(columnName4);
            param3 = rs.getDate(columnName3);
            param2 = rs.getInt(columnName2);
            param1 = rs.getInt(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }
    }
    // Получить всю таблицу Readers
    private static void getReaders(Connection connection) throws SQLException {
        String columnName0 = "reader_id", columnName1 = "firs_name", columnName2 = "last_name", columnName3 = "email", columnName4 = "phone_number";
        int param0 = -1, param4 = -1;
        String param1 = null, param2 = null, param3 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM readers;");

        while (rs.next()) {
            param4 = rs.getInt(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }
    }

    // Получить книги, написанные после даты
    private static void getBookByDate(Connection connection, Date value1) throws SQLException {
            long time = System.currentTimeMillis();
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT title " +
                            "FROM books " +
                            "WHERE published_year < value1");
            statement.setDate(1, value1);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " ms)");
    }
    // Получить всех авторов, которые написали более одной книги
    private static void getAuthorsBooks(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "SELECT Authors.first_name, Authors.last_name, COUNT(Books.book_id) AS books_count\n" +
                        "FROM Authors" +
                        "JOIN Books ON Authors.author_id = Books.author_id" +
                        "GROUP BY Authors.author_id" +
                        "HAVING COUNT(Books.book_id) > 1;");
        ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2));
        }
    }

    // Вывод книг которые ни разу не брали
    private static void getNoLoansBook(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "SELECT title" +
                "FROM Books" +
                "WHERE book_id NOT IN (SELECT book_id FROM Loans);");
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
    // Получить количество копий каждой книги
    private static void getCopiesEachBook(Connection connection) throws SQLException{
        PreparedStatement statement = connection.prepareStatement(
                "SELECT title, copies_available" +
                        "FROM Books;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()){
            System.out.println(rs.getInt(1));
        }
    }

    public static void getCountAuthorsBooks(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
                "SELECT Authors.first_name, Authors.last_name, COUNT(Books.book_id) AS books_count" +
                        "FROM Authors" +
                        "LEFT JOIN Books ON Authors.author_id = Books.author_id" +
                        "GROUP BY Authors.author_id;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()){
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getInt(3));
        }
    }
    // Получить информацию о всех книгах, которые были взяты за последний месяц
    public static void  getMonthBooks(Connection connection) throws SQLException{
        PreparedStatement statement = connection.prepareStatement(
                "SELECT Books.title, Loans.loan_date, Loans.return_date" +
                        "FROM Books" +
                        "JOIN Loans ON Books.book_id = Loans.book_id" +
                        "WHERE Loans.loan_date >= CURDATE() - INTERVAL 1 MONTH;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()){
            System.out.println(rs.getString(1) + " | " + rs.getDate(2) + " | " +rs.getDate(3));
        }
    }
    // Получить читателей у которых есть не сданные книги
    public static void getReadersWithAnyBooks(Connection connection) throws SQLException{
        PreparedStatement statement = connection.prepareStatement(
                "SELECT DISTINCT Readers.first_name, Readers.last_name" +
                        "FROM Readers" +
                        "JOIN Loans ON Readers.reader_id = Loans.reader_id" +
                        "WHERE Loans.return_date IS NULL;");
        ResultSet rs = statement.executeQuery();

        while (rs.next()){
            System.out.println(rs.getString(1) + " | " + rs.getString(2));
        }
    }
    // JOIN Получить всех читателей, взявших конкретную книгу
    private static void getInterestReadersWithThisBooks(Connection connection, String title) throws SQLException{
        if (title == null || title.isBlank()) return;
        title = '%' + title + '%';
        PreparedStatement statement = connection.prepareStatement(
                "SELECT readers.first_name, readers.last_name " +
                        "FROM readers " +
                        "JOIN Loans ON readers.reader_id = loans.reader_id " +
                        "JOIN Book ON Loans.book_id = Books.book_id " +
                        "WHERE Book.title LIKE ?;");
        statement.setString(1, title);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2));
        }
    }

    // INSERT Добавление нового читателя
    private static void addReader(Connection connection,String first_name, String last_name, String email, String phone_number)  throws SQLException {
        if (first_name == null || first_name.isBlank() || last_name.isBlank() || email.isBlank() || phone_number == null) {
            return;
        }

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO readers(first_name, last_name, email, phone_number) VALUES (?, ?, ?, ?) returning reader_id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, first_name);
        statement.setString(2, last_name);
        statement.setString(3, email);
        statement.setString(4, phone_number);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор читателя " + rs.getInt(1));
        }
        System.out.println("INSERTed " + count + " readers");
        getReaders(connection);
    }
    // INSERT Добавление новой книги
    private static void addBook(Connection connection,String title, int author_id, Date published_year, int copies_avalieble)  throws SQLException {
        if ((title == null) || published_year == Date.valueOf("0000-00-00")  || (copies_avalieble < 0) || (author_id < 0)) {
            return;
        }

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO books(title, author_id, published_year, copies_avalieble) VALUES (?, ?, ?, ?) returning book_id;", Statement.RETURN_GENERATED_KEYS);
        statement.setString(1, title);
        statement.setInt(2, author_id);
        statement.setDate(3, published_year);
        statement.setInt(4, copies_avalieble);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор книги " + rs.getInt(1));
        }
        System.out.println("INSERTed " + count + " books");
        getReaders(connection);
    }

}
