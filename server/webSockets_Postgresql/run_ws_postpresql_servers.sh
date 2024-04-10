
#Start postpresql server
echo "Starting Postgresql Server"
sudo systemctl start postgresql-13
sudo systemctl enable postgresql-13

sleep 5

# Create the database if it doesn't exist
python3 create_db.py

