import mysql.connector
import math

# DBED Assignment 2
# Student ID: a1924315
# Name: Aliv Rheantoro

class DBEDAssign2():
    def __init__(self,name):
        self.name=name

    def disp(self):
        print(self.name)

    def setUp(self):
        """Set up the DB connection to the 'postal' database"""
        self.connection = mysql.connector.connect(database='postal')
        self.cursor = self.connection.cursor(buffered=True)

    def syncDB(self):
        """We need to commit after insertion to ensure that the changes stick.
        syncDB is a wrapper to the connection commit that takes no parameters and
        returns no result."""
        self.connection.commit()

    def tearDown(self):
        """tearDown destroys the cursor and the connection to clean up."""
        self.cursor.close()
        self.connection.close()

    def show_all(self):
        """Select all rows in the database's pcode table and return it as a list to the user"""
        query = "SELECT * from pcode;"
        self.cursor.execute(query,)
        return self.cursor.fetchall()


    def select_by_pcode(self,pcode):
        """Perform a SELECT * query using the pcode parameter for postcode. Returns the query
        result as a list object."""
        query = "SELECT * FROM pcode WHERE postcode = %s;"
        self.cursor.execute(query, (pcode,))
        return self.cursor.fetchall()


    def insert_data(self,pcode,locality,state):
        """Insert data into the database"""
        query = "INSERT INTO pcode (postcode, locality, state) VALUES (%s, %s, %s);"
        self.cursor.execute(query, (pcode, locality, state))


    def readData(self,fname):
        """Read in the data from the CSV datafile called fname and put it into the database
        Takes a single string parameter and does not return any values.
        IMPORTANT: you must call syncDB before exiting or your changes won't stick!"""
        with open('./'+fname,"r") as csv:
            # Skip the header
            csv.readline()

            # Your code here to insert the data
            for line in csv:
                parts = [p.strip() for p in line.split(",")]
                if len(parts) != 3:
                    continue

                pcode, locality, state = parts
                self.insert_data(pcode, locality, state)
                row_count +=1

            csv.close()
            #Commit
            self.syncDB()

    def entropyCalc(self):
        """Analyse the postcode data to determine the entropy of the fourth column
        Takes no parameters and returns a single floating point number that is the
        total entropy of the fourth column.
        """

        # Scan the database for the counts
        self.cursor.execute("SELECT postcode FROM pcode")
        rows = self.cursor.fetchall()
        fourth_digits = [row[0][3] for row in rows if len(row[0]) == 4]
        counts = {}
        for digit in fourth_digits:
            counts[digit] = counts.get(digit, 0) + 1

        # Calculate the frequencies and total entropy
        total = len(fourth_digits)
        probabilities = [count / total for count in counts.values()]
        entropy = -sum(p * math.log2(p) for p in probabilities if p > 0)

        # Return the total entropy
        return entropy