# ETL Data Warehouse Project with Medallion architecture for mortgage loan analytics
<img width="3433" height="1393" alt="Blank diagram - Page 1 (1)" src="https://github.com/user-attachments/assets/3d8649a6-ecd6-4feb-a71d-59f15bdd6815" />

End-to-end mortgage loan analytics platform built using AWS and Terraform. Implements a Medallion Architecture (Bronze → Silver → Gold) on PostgreSQL (RDS), with data ingestion from S3 → AWS Glue → RDS, and visualization via Power BI.

## Project Setup 

| Layer                    | Technology               | Description                                                            |
| ------------------------ | ------------------------ | ---------------------------------------------------------------------- |
| **Infrastructure**       | Terraform                | Creates AWS VPC, subnets, security groups, and RDS PostgreSQL instance |
| **Raw Data**    | AWS S3                   | Stores mock mortgage loan data                                         |
| **ETL / Transformation** | AWS Glue (Crawler + Job) | Cleans, enriches, and loads data into RDS schemas                      |
| **Data Warehouse (RDS)** | PostgreSQL               | Stores structured data in Bronze, Silver, and Gold schemas             |
| **Visualization**        | Power BI                 | Connects to RDS to display KPIs and trends                             |

## Workflow Checklist 
1. Load the **mortgage_loan_dataset** into s3 data storage
2. Author terraform IAC to create VPC, subnets, security groups 
3. Provision RDS PostgresSQL instance
4. Create Bronze, Silver and Gold Schema
5. Launch glue, create a new crawler to extract tables from s3 storage and store the tables in glue-s3-data catalog
6. Create another crawler to extract the bronze schema
7. Data Ingestion - Create an ETL job to extract data from s3-data-catalog (source) and load to rds-data-catalog (target)
8. Load the raw data to bronze schema in RDS
9. Write sql transformations and data normalization to silver and gold layer
10. Data visualization with powerBI

## Step 1: Load the **mortgage_loan_dataset** into s3 data storage 
In this project, I used boto3 AWS SDK for Python, to programmatically upload our mortgage loan dataset to an Amazon S3 bucket

``` python
import boto3
import os 
from dotenv import load_dotenv 
from boto3.exceptions import Boto3Error

# load environment variables from .env file
load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID") 
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY") 
region_name = os.getenv("REGION_NAME")
s3_bucket = os.getenv("S3_BUCKET")
s3_folder = os.getenv("S3_FOLDER")
    


#create s3 client 

s3 = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key, 
    region_name = region_name   
)

# Create an s3 bucket 
bucket = s3.create_bucket(Bucket = s3_bucket) 

# specify the file name and the s3 path 
file_name = 'mortgage_loan_mock_data.csv'
s3_path = f"{s3_folder}/{file_name}" if s3_folder else file_name

# Upload the csv file to the s3 bucket 
try: 
    s3.upload_file(file_name, s3_bucket, s3_path)
    print(f'✔ File upload to: {s3_bucket}/{s3_path}')
except Boto3Error as e:
    print("❌ An error occured while uploading")
    print(e)
    

# Add a validation 

try:
    s3.head_object(Bucket=s3_bucket, Key= s3_path)
    print(f'✔ validation successful: {file_name} exists in {s3_bucket}/{s3_path} ')
    
except Boto3Error as e:
    print(f'❌ validation failed: {file_name} does not exists in {s3_bucket}/{s3_path}')
    print(e) 

```
## Step 2: Author terraform IAC to create VPC, subnets, security groups 
``` hcl
# virtual private cloud (vpc)

resource "aws_vpc" "manjaro_db" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = {
    Name = "${var.project}-vpc"
  }
}

# add internet gateway 
resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.manjaro_db.id
}

# create route tables for subnets 
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.manjaro_db.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.project}-public-rt"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.manjaro_db.id
  tags   = { Name = "${var.project}-private-rt" }
}

# associate route table with each public subnet 
resource "aws_route_table_association" "public_assoc" {
  count          = length(aws_subnet.public)
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private_assoc" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# add a public subnet 
resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.manjaro_db.id
  cidr_block              = cidrsubnet(aws_vpc.manjaro_db.cidr_block, 8, count.index + 1)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  lifecycle {
    create_before_destroy = true
  }
  tags = { Name = "${var.project}-public-${count.index}" }


}

# add a private subnet 
resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.manjaro_db.id
  cidr_block        = cidrsubnet(aws_vpc.manjaro_db.cidr_block, 8, count.index + 11)
  availability_zone = data.aws_availability_zones.available.names[count.index]

  lifecycle {
    create_before_destroy = true
  }

  tags = { Name = "${var.project}-private-${count.index}" }


}

# add security group 
resource "aws_security_group" "manjaro_db_sg" {
  name   = "${var.project}-manjaro_db_sg"
  vpc_id = aws_vpc.manjaro_db.id

  ingress {
    description = "Postgres"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    self        = true
    cidr_blocks = [aws_vpc.manjaro_db.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
```
## Step 3: Provision RDS PostgresSQL instance 
``` hcl
resource "aws_db_subnet_group" "manjaro_rds_subnet_group" {
  name       = "${var.project}-db-subnets"
  subnet_ids = aws_subnet.public[*].id

  tags = {
    Name = "${var.project}-db-subnet-group"
  }
}

resource "aws_db_instance" "mortgage_rds" {
  identifier             = "${var.project}-postgres"
  allocated_storage      = 20
  engine                 = "postgres"
  engine_version         = "17.2"
  instance_class         = var.rds_instance_class
  db_name                = "manjaro_mortgage_db"
  username               = var.db_username
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.manjaro_rds_subnet_group.name
  vpc_security_group_ids = [aws_security_group.manjaro_db_sg.id]
  skip_final_snapshot    = true
  publicly_accessible    = true

  tags = { Name = "${var.project}-rds" }
}
```
### Test connection with aws RDS instance 

<img width="950" height="184" alt="test-connection" src="https://github.com/user-attachments/assets/4bf36bc5-8bc5-4132-a451-c597de68649f" />

## Step 4: Create Bronze, Silver and Gold Schema 

``` sql
create schema bronze; 
create schema silver; 
create schema gold;

#create table in the bronze schema
drop table if exists bronze.mortgage_loans;
create table bronze.mortgage_loans (
	credit_score INT, 
	gender VARCHAR(20), 
	payment_to_income_ratio FLOAT, 
	days_delinquent INT, 
	payment_status VARCHAR(40), 
	loan_to_value_ratio FLOAT, 
	loan_amount FLOAT,
	maturity_date VARCHAR(45), 
	property_value FLOAT,
	loan_purpose VARCHAR(60),
	marital_status VARCHAR(30), 
	borrower_id VARCHAR(40), 
	origination_date VARCHAR(45), 
	loan_term INT, 
	interest_rate FLOAT,
	current_balance FLOAT, 
	equity_built FLOAT, 
	employment_status VARCHAR(40), 
	state VARCHAR(15), 
	default_flag INT
);

```
<img width="939" height="177" alt="schema" src="https://github.com/user-attachments/assets/d5fd1f50-3af3-4ff5-af17-5c051e7d26c9" /> 

## Step 5: Launch glue, create a new crawler to extract tables from s3 storage and store the tables in glue-s3-data catalog
<img width="1239" height="569" alt="GLUE-S3-CRAWLER" src="https://github.com/user-attachments/assets/945d24ba-1b71-43b9-82f2-2dae227e128d" /> 

## Step 6: Create another crawler to extract the bronze schema 

<img width="1248" height="596" alt="jdbc-crawler" src="https://github.com/user-attachments/assets/874195ab-a02c-44a2-b306-9bcb8e0a7d4a" />

## Step 7: Data Ingestion - Create an ETL job to extract data from s3-data-catalog (source) and load to rds-data-catalog (target)
<img width="1515" height="521" alt="manjaro-data-ingest-b" src="https://github.com/user-attachments/assets/0d920476-7be5-477f-9ed1-317078ac769a" />
<img width="1490" height="478" alt="manjaro-data-ingest-a" src="https://github.com/user-attachments/assets/637ec0c5-37e6-45d1-a100-c9a78f13ea74" />

