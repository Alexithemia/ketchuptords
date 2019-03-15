# Requirements
* Serverless
* AWS account
  * SQS queue
  * RDS database

# Setup
1. ` npm i `
2. Rename serverless-sample.yml to serverless.yml
3. Input data from SQS and RDS into serverless.yml according to comments
4. Run SQL commands below in RDS database if you have not already created these tables.
5. ` serverless deploy `

# SQL Commands 
### Create required tables
```
CREATE TABLE clients (id SERIAL PRIMARY KEY, name VARCHAR(40), username VARCHAR(25) NOT NULL, password TEXT NOT NULL, key VARCHAR(50) NOT NULL, created_at TIMESTAMP with TIME ZONE DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP with TIME ZONE DEFAULT CURRENT_TIMESTAMP, public BOOLEAN DEFAULT TRUE NOT NULL);

CREATE TABLE events (id SERIAL PRIMARY KEY, user_name VARCHAR(20) NOT NULL, metric VARCHAR(20) NOT NULL, value DECIMAL NOT NULL, client_id INTEGER NOT NULL, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL, public BOOLEAN DEFAULT TRUE NOT NULL);
```