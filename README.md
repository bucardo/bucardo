# Schema Migration Steps

This document outlines the necessary steps to perform schema migration using Bucardo. It includes steps for preparing the code, installing and configuring Bucardo, and adding databases and tables.

## Table of Contents

1. [Preparing the Bucardo Code](#1-preparing-the-bucardo-code)
2. [Bucardo Installation Prerequisites](#2-bucardo-installation-prerequisites)
3. [Custom Bucardo Installation](#3-custom-bucardo-installation)
4. [Bucardo Starting Steps](#4-bucardo-starting-steps)
5. [Bucardo Syncing](#5-bucardo-syncing)
6. [Additional Resources](#6-additional-resources)

## 1. Preparing the Bucardo Code

1. **Clone the Bucardo repository from GitHub:**
   ```bash
   git clone https://github.com/srinivasanmohankumar/bucardo.git --branch schema-migration-v3

2. **Modify the bucardo.schema file:**

    **Locate and change the project ID at line 2426:**

    ```perl
    my $project_id_var = 1597; 
    # Change this to your desired project ID that Needs to be Migrated This will be applied to all triggers
