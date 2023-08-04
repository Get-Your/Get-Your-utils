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

from psycopg2 import connect as psqlconnect
from psycopg2 import sql
from tomlkit import loads
from tomlkit import exceptions as tomlexceptions
from pathlib import Path
import json
from rich.prompt import Prompt, Confirm

import coftc_cred_man as crd

## Initialize vars
try:
    fileDir = Path(__file__).parent
except NameError:   # dev
    fileDir = Path.cwd()
    
with open(
    fileDir.parent.parent.joinpath('.env.deploy'),
    'r',
    encoding='utf-8',
    ) as f:
        secrets_dict = loads(f.read())
    
def get_secret(var_name, read_dict=secrets_dict):
    '''Get the secret variable or return explicit exception.'''
    try:
        return read_dict[var_name]
    except tomlexceptions.NonExistentKey:
        error_msg = f"Set the '{var_name}' secrets variable"
        raise tomlexceptions.NonExistentKey(error_msg)

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
    
oldEmail = Prompt.ask("Enter the email address of the user to clone")
    
newEmail = Prompt.ask("Enter an email address for the cloned user (note that this email may be sent communications from the app)")

passwordClone = get_secret('PASSWORD_CLONE_ACCOUNT')
print(
    "\nThe password will be the same as user '{}'\n".format(
        passwordClone,
        )
    )


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
queryStr = sql.SQL(
    "select {fd} from {tbl} where lower({idfd})=%s"
    ).format(
        fd=sql.SQL(', ').join(map(sql.Identifier, ['id'])),
        tbl=sql.Identifier('public', 'app_user'),
        idfd=sql.Identifier('email'),
        )
srcCursor.execute(queryStr, (oldEmail.lower(),))
userId = [x[0] for x in srcCursor.fetchall()]
if len(userId)>1:
    raise AttributeError("More than one id exists for this user")
userId = userId[0]

# Check if user exists in target (dev) database
queryStr = sql.SQL(
    "select count(*) from {tbl} where {idfd}=%s"
    ).format(
        tbl=sql.Identifier('public', 'app_user'),
        idfd=sql.Identifier('id'),
        )
targetCursor.execute(queryStr, (userId,))
userExists = True if targetCursor.fetchone()[0]>0 else False

if userExists:
    queryStr = sql.SQL(
        "select {fd} from {tbl} where {idfd}=%s"
        ).format(
            fd=sql.SQL(', ').join(map(sql.Identifier, ['email'])),
            tbl=sql.Identifier('public', 'app_user'),
            idfd=sql.Identifier('id'),
            )
    targetCursor.execute(queryStr, (userId,))
    duplicateEmail = targetCursor.fetchone()[0]
    
    userConfirm = Confirm.ask(
        f"User exists in dev tables (under [green]{duplicateEmail}[/green]). Okay to overwrite?"
        )
    
    if not userConfirm:
        raise KeyboardInterrupt("Cancelled by user")
        
        
# Get the encrypted password of the target (in case this is the duplicate user)
queryStr = sql.SQL(
    "select {fd} from {tbl} where {idfd}=%s"
    ).format(
        fd=sql.SQL(', ').join(map(sql.Identifier, ['password'])),
        tbl=sql.Identifier('public', 'app_user'),
        idfd=sql.Identifier('email'),
        )
targetCursor.execute(queryStr, (passwordClone,))
targetPassword = targetCursor.fetchone()[0]

# Go through tables. Note that these are in the order in which they are written
# in the app (also ensure 'app_user' is first, for foreign key constraints)
tableList = [
    'app_user',
    'app_address',
    'app_household',
    'app_householdmembers',
    'app_eligibilityprogram',
    'app_iqprogram',
    
    'app_userhist',
    'app_addresshist',
    'app_householdhist',
    'app_householdmembershist',
    'app_eligibilityprogramhist',
    'app_iqprogramhist',
    ]

# Remove the user from target (if exists) so as to get a fresh start
if userExists:
    # Loop through tableList backward and delete
    for table in reversed(tableList):
        
        if table == 'app_user':
            idField = 'id'
        else:
            idField = 'user_id'
        
        queryStr = sql.SQL(
            "delete from {tbl} where {idfd}=%s"
            ).format(
                tbl=sql.Identifier('public', table),
                idfd=sql.Identifier(idField),
                )
        targetCursor.execute(queryStr, (userId,))
        
    # Commit all deletions
    targetConn.commit()

# Copy user from source to target databases
for table in tableList:
    
    queryStr = sql.SQL(
        "select {fd} from {tbl} where {tbfd}=%s and {idfd}!='id'"
        ).format(
            fd=sql.SQL(', ').join(map(sql.Identifier, ['column_name'])),
            tbl=sql.Identifier('information_schema', 'columns'),
            tbfd=sql.Identifier('table_name'),
            idfd=sql.Identifier('column_name'),
            )
    srcCursor.execute(queryStr, (table,))
    fieldList = [x[0] for x in srcCursor.fetchall()]
    
    if table == 'app_user':
        idField = 'id'
    else:
        idField = 'user_id'
    
    # Gather source data
    queryStr = sql.SQL(
        "select {fd} from {tbl} where {idfd}=%s"
        ).format(
            fd=sql.SQL(', ').join(map(sql.Identifier, fieldList)),
            tbl=sql.Identifier('public', table),
            idfd=sql.Identifier(idField),
            )
    srcCursor.execute(queryStr, (userId,))
    try:
        # Convert inner tuples to lists for mutability
        dbOut = [list(x) for x in srcCursor.fetchall()]
    except TypeError as err:   # should be due to no records existing
        print(
            "Error copying table '{}': {}.".format(
                table,
                err,
                )
            )
        continue
    
    if len(dbOut) == 0:
        continue
    
    # Alter email and password, if applicable
    if table == 'app_user':
        
        # Should only be one record (that will be modified below)
        if len(dbOut) > 1:
            raise TypeError("There should only be one app_user record")
        
        # Set email to new version
        dbOut[0][fieldList.index('email')] = newEmail
        
        # Set to password value to the target password gathered above
        dbOut[0][fieldList.index('password')] = targetPassword
        
        # Change phone number to unused (to prevent notifications)
        dbOut[0][fieldList.index('phone_number')] = '+13035551234'
        
    # Ensure the matching address(es) exist and use the target IDs
    elif table == 'app_address':
        
        # Should only be one record (that will be modified below)
        if len(dbOut) > 1:
            raise TypeError("There should only be one app_address record")
        
        for addtype in ['eligibility_address_id', 'mailing_address_id']:
            # Gather address
            queryStr = sql.SQL(
                "select {fd} from {tbl} where {idfd}=%s"
                ).format(
                    fd=sql.SQL(', ').join(map(sql.Identifier, ['address_sha1'])),
                    tbl=sql.Identifier('public', 'app_addressrd'),
                    idfd=sql.Identifier('id'),
                    )
            srcCursor.execute(queryStr, (dbOut[0][fieldList.index(addtype)],))
            sha1Val = srcCursor.fetchone()[0]
            
            # Take the address ID from the target if exists; else create and
            # use that ID
            queryStr = sql.SQL(
                "select {fd} from {tbl} where {idfd}=%s"
                ).format(
                    fd=sql.SQL(', ').join(map(sql.Identifier, ['id'])),
                    tbl=sql.Identifier('public', 'app_addressrd'),
                    idfd=sql.Identifier('address_sha1'),
                    )
            targetCursor.execute(queryStr, (sha1Val,))
            try:
                targetAddrId = targetCursor.fetchone()[0]
            except TypeError:   # address DNE; add it
                # Get AddressRD info
                queryStr = sql.SQL(
                    "select {fd} from {tbl} where {tbfd}=%s and {idfd}!='id'"
                    ).format(
                        fd=sql.SQL(', ').join(map(sql.Identifier, ['column_name'])),
                        tbl=sql.Identifier('information_schema', 'columns'),
                        tbfd=sql.Identifier('table_name'),
                        idfd=sql.Identifier('column_name'),
                        )
                srcCursor.execute(queryStr, ("app_addressrd",))
                addrFieldList = [x[0] for x in srcCursor.fetchall()]
                
                # Gather source data
                srcCursor.execute(
                    sql.SQL(
                        """select {fd} from {tbl} where "id"=%s"""
                        ).format(
                            fd=sql.SQL(', ').join(map(sql.Identifier, addrFieldList)),
                            tbl=sql.Identifier('public', 'app_addressrd'),
                            ),
                            (dbOut[0][fieldList.index(addtype)],),
                        )
                srcAddrOut = list(srcCursor.fetchone())
                
                # Insert address into target DB and return the proper ID
                queryStr = sql.SQL(
                    "insert into {tbl} ({fd}) VALUES ({vl}) returning ID"
                    ).format(
                        fd=sql.SQL(', ').join(map(sql.Identifier, addrFieldList)),
                        tbl=sql.Identifier('public', 'app_addressrd'),
                        vl=sql.SQL(', ').join(sql.Placeholder()*len(addrFieldList)),
                        )
                targetCursor.execute(queryStr, srcAddrOut)
                targetAddrId = targetCursor.fetchone()[0]
                
                # Commit this insert so the foreign keys will behave
                targetConn.commit()
                
            # Use the target ID instead of the source (regardless of the insert)
            dbOut[0][fieldList.index(addtype)] = targetAddrId
    
    # Insert into the target table
    if idField == 'id':
        # ID is the primary key and is ignored above, so must be added here
        queryStr = sql.SQL(
            "insert into {tbl} ({fd}) VALUES {vl}"
            ).format(
                fd=sql.SQL(', ').join(map(sql.Identifier, fieldList+[idField])),
                tbl=sql.Identifier('public', table),
                vl=sql.SQL(', ').join(sql.Placeholder()*len(dbOut)),
                )
        
        targetCursor.execute(
            queryStr,
            # JSONify any dicts and convert back to list of tuples
            [tuple([json.dumps(x) if isinstance(x, dict) else x for idx,x in enumerate(elem)]+[userId]) for elem in dbOut],
            )
        
    else:
        queryStr = sql.SQL(
            "insert into {tbl} ({fd}) VALUES {vl}"
            ).format(
                fd=sql.SQL(', ').join(map(sql.Identifier, fieldList)),
                tbl=sql.Identifier('public', table),
                vl=sql.SQL(', ').join(sql.Placeholder()*len(dbOut)),
                )
        targetCursor.execute(
            queryStr,
            # JSONify any dicts and convert back to list of tuples
            [tuple([json.dumps(x) if isinstance(x, dict) else x for idx,x in enumerate(elem)]) for elem in dbOut],
            )
            
targetConn.commit()
    
print('User cloned!')
print('email: {}'.format(newEmail))
print('password: same as {}'.format(passwordClone))

