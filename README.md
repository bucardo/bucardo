# Schema Migration Steps

This document outlines the necessary steps to perform schema migration using Bucardo. It includes steps for preparing the code, installing and configuring Bucardo, and adding databases and tables.

## Table of Contents

1. [Bucardo Installation Prerequisites](#2-bucardo-installation-prerequisites)
2. [Preparing the Bucardo Code](#1-preparing-the-bucardo-code)
4. [Custom Bucardo Installation](#3-custom-bucardo-installation)
3. [Bucardo Starting Steps](#4-bucardo-starting-steps)
4. [Bucardo Syncing](#5-bucardo-syncing)
5. [Additional Resources](#6-additional-resources)

## 1. Bucardo Installation Prerequisites

1.1 **Install PostgreSQL and its development tools:**
    ```bash
    sudo apt update
    sudo apt install postgresql libpq-dev postgresql-server-dev-16 postgresql-client-16

1.2 **Set up the PostgreSQL user as sudo User**
    ```bash
    usermod -aG sudo postgres

1.2.1    **Set the password for the postgres user as "postgres":**
    ```bash
    passwd postgres




## 2. Preparing the Bucardo Code

1. **Clone the Bucardo repository from GitHub:**
   ```bash
   git clone https://github.com/srinivasanmohankumar/bucardo.git --branch schema-migration-v3

2. **Modify the bucardo.schema file:**

    **Locate and change the project ID at line 2426: Change this to your desired project ID that Needs to be Migrated This will be applied to all triggers**

    ```perl
    my $project_id_var = 1597; 
    # Change this to your desired project ID that Needs to be Migrated This will be applied to all triggers
