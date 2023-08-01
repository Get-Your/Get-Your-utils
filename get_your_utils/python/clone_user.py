"""
Get-Your-utils consists of utility scripts for the Get-Your
application, used primarily by the City of Fort Collins.
Copyright (C) 2023

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import os
from psycopg2 import connect as psqlconnect
from psycopg2 import sql, extensions
from dotenv import load_dotenv
from pathlib import Path

import coftc_cred_man as crd

try:
    fileDir = Path(__file__).parent
except NameError:   # dev
    fileDir = Path.cwd()


raise Exception("DON'T USE THIS SCRIPT UNTIL THE REARCHITECTURE/REFACTOR IS COMPLETE")

## Initialize vars
load_dotenv(fileDir.parent.joinpath('.env'))
PASSWORD_CLONE_ACCOUNT = os.getenv('PASSWORD_CLONE_ACCOUNT')

# # Gather source and target databases (target is always dev)
# dbSourceEnv = input("Enter the database environment (prod or dev): ")

dbSourceEnv = 'prod'
if dbSourceEnv.lower() == 'prod':
    srcProfile = 'getfoco_prod'
elif dbSourceEnv.lower() == 'dev':
    srcProfile = 'getfoco_dev'
else:
    raise TypeError('Database environment not recognized')
targetProfile = 'getfoco_dev'
    
oldEmail = input("Enter the email address of the user to clone: ")
    
newEmail = input("Enter an email address for the cloned user: ")

passwordClone = PASSWORD_CLONE_ACCOUNT
print("\nThe password will be the same as user '{}' on getfoco.fcgov.com\n".format(
    passwordClone,
    ))


# Connect to the dbs
srcCred = crd.Cred(srcProfile)

# Construct connection string and connect
srcConn = psqlconnect(
    "host={hst} user={usr} dbname={dbn} password={psw} sslmode={ssm}".format(
        hst=srcCred.config['host'],
        usr=srcCred.config['user'],
        dbn=srcCred.config['db'],
        psw=srcCred.password(),
        ssm='require')
    )  
srcCursor = srcConn.cursor()

targetCred = crd.Cred(targetProfile)

# Construct connection string and connect
targetConn = psqlconnect(
    "host={hst} user={usr} dbname={dbn} password={psw} sslmode={ssm}".format(
        hst=targetCred.config['host'],
        usr=targetCred.config['user'],
        dbn=targetCred.config['db'],
        psw=targetCred.password(),
        ssm='require')
    ) 
targetCursor = targetConn.cursor()

# Gather user id from source table
srcCursor.execute(f"select id from application_user where email='{oldEmail}'")
userId = [x[0] for x in srcCursor.fetchall()]
if len(userId)>1:
    raise AttributeError("More than one id exists for this user")

userId = userId[0]

# Check if user exists in target (dev) database
targetCursor.execute(
    sql.SQL(
        "select count(*) from application_user where id={}".format(
            userId,
            )
        )
    )
userExists = True if targetCursor.fetchone()[0]>0 else False
if userExists:
    
    targetCursor.execute(
        sql.SQL(
            """select "email" from application_user where id=%s"""
            ),
        (userId,),
        )
    duplicateEmail = targetCursor.fetchone()[0]
    
    userInput = input(
        "User exists in dev tables (under {}). Okay to overwrite? (Y/n): ".format(
            duplicateEmail,
            )
        )
    
    if userInput.lower() not in ('', 'y'):
        raise KeyboardInterrupt("Cancelled by user")
        


# Go through tables
# The file-related tables (that can have multiple files - and therefore
# records) are commented out, as these shouldn't affect anything that would
# be tested by cloning

# Ensure 'application_user' is first, for foreign key constraints
tableList = [
    'application_user',
    'application_moreinfo',
    'application_programs',
    'application_addressverification',
    'application_attestations',
    'application_eligibility',
    # 'application_user_address_files',
    # 'application_user_files',
    'dashboard_taxinformation',
    'application_addresses',
    'dashboard_form',
    # 'dashboard_residencyform',
    ]

# Copy user from source to target databases
for table in tableList:
    
    srcCursor.execute(
        sql.SQL(
            "SELECT column_name FROM information_schema.columns WHERE table_name=%s and column_name!='id'"
            ).format(
                tbl=sql.Identifier(table),
                ),
            (table,),
        )
    fieldList = [x[0] for x in srcCursor.fetchall()]
    
    if table in ('application_user_files', 'application_user_address_files'):
        idField = 'user_id'
    elif table == 'application_user':
        idField = 'id'
    else:
        idField = 'user_id_id'
    
    # Gather source data
    srcCursor.execute(
        sql.SQL(
            "select {fd} from {tbl} where {idfd}=%s"
            ).format(
                fd=sql.SQL(', ').join(map(sql.Identifier, fieldList)),
                tbl=sql.Identifier(table),
                idfd=sql.Identifier(idField),
                ),
                (userId,),
            )
    
    try:
        dbOut = list(srcCursor.fetchone())
    except TypeError as err:   # should be due to no records existing
        print(
            "Error copying table '{}': {}.".format(
                table,
                err,
                )
            )
        continue
    
    # Alter email and password, if applicable
    if table == 'application_user':
        
        dbOut[fieldList.index('email')] = newEmail
        
        srcCursor.execute(
            sql.SQL(
                """select "password" from {tbl} where {emlfd}=%s"""
                ).format(
                    tbl=sql.Identifier(table),
                    emlfd=sql.Identifier('email'),
                    ),
                    (passwordClone,),
            )
        dbOut[fieldList.index('password')] = srcCursor.fetchone()[0]
        
        # Change phone number to unused (to prevent notifications)
        dbOut[fieldList.index('phone_number')] = '+13035551234'
        
    
    if userExists:  # use an update instead of insert
        targetCursor.execute(
            sql.SQL(
                "update {tbl} set {fdvl} where {idfd}={idvl}"
                ).format(
                    tbl=sql.Identifier(table),
                    fdvl=sql.SQL(', ').join(sql.Composed([sql.Identifier(x), sql.SQL('='), sql.Placeholder()]) for x in fieldList),
                    idfd=sql.Identifier(idField),
                    idvl=sql.Placeholder(),
                    ),
            # dependentInformation is a weird one, so use AsIs for it
            [f'"{extensions.AsIs(x)}"' if fieldList[idx]=='dependentInformation' else x for idx,x in enumerate(dbOut)]+[userId]
            )

    else:
        if idField == 'id':     # this is the primary key and is ignored above
            targetCursor.execute(
                sql.SQL(
                    "insert into {tbl} ({fd}) VALUES ({vl}) "
                    ).format(
                        tbl=sql.Identifier(table),
                        fd=sql.SQL(', ').join(map(sql.Identifier,fieldList+[idField])),
                        vl=sql.SQL(', ').join(sql.Placeholder()*len(fieldList+[idField])),
                        ),
                # dependentInformation is a weird one, so use AsIs for it
                [f'"{extensions.AsIs(x)}"' if fieldList[idx]=='dependentInformation' else x for idx,x in enumerate(dbOut)]+[userId],
                )
            
        else:
            targetCursor.execute(
                sql.SQL(
                    "insert into {tbl} ({fd}) VALUES ({vl})"
                    ).format(
                        tbl=sql.Identifier(table),
                        fd=sql.SQL(', ').join(map(sql.Identifier,fieldList)),
                        vl=sql.SQL(', ').join(sql.Placeholder()*len(fieldList)),
                        ),
                # dependentInformation is a weird one, so use AsIs for it
                [f'"{extensions.AsIs(x)}"' if fieldList[idx]=='dependentInformation' else x for idx,x in enumerate(dbOut)],
                )
            
    targetConn.commit()
    
print('User cloned!')
print('email: {}'.format(newEmail))
print('password: same as {}'.format(passwordClone))

