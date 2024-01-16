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

from psycopg2 import sql
from tomlkit import loads
from tomlkit import exceptions as tomlexceptions
from pathlib import Path
import json
from rich.prompt import Prompt, Confirm
from rich import print
import sqlite3
import decimal

from run_extracts import GetFoco

from psycopg2.extensions import connection as pg_connection
from sqlite3 import Connection as sqlite_connection

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
    """ Get the secret variable or return explicit exception. """
    try:
        return read_dict[var_name]
    except tomlexceptions.NonExistentKey:
        error_msg = f"Set the '{var_name}' secrets variable"
        raise tomlexceptions.NonExistentKey(error_msg)
        
        
def conn_info(conn):
    """ Determine if database connection is active and its type. """
    
    # Initialize output dict
    infoDict = {}
    
    # No connection
    if conn is None:
        infoDict.update(
            {
                'is_active': False,
                'type': '',
                'environment': '',
            }
        )

    # Postgres connection
    elif isinstance(conn, pg_connection):
        infoDict.update({'type': 'pg'})
        if not conn.closed:
            dbName = conn.get_dsn_parameters()['dbname'].lower()
            infoDict.update(
                {
                    'is_active': True,
                    'environment': 'dev' if 'dev' in dbName else 'prod' if 'prod' in dbName else 'unknown',
                }
            )
        else:
            infoDict.update(
                {
                    'is_active': False,
                    'environment': 'unknown',
                }
            )
        
    # SQLite connection
    elif isinstance(conn, sqlite_connection):
        infoDict.update(
            {
                'type': 'sqlite',
                'environment': 'local',
            }
        )
        try:
            # Try to set a new cursor, then close it if successful
            testCursor = conn.cursor()
            testCursor.close()
            infoDict.update({'is_active': True})
        except:
            infoDict.update({'is_active': False})
        
    else:
        raise Exception
        
    return(infoDict)
        

def run_clone(
        source_profile: str,
        target_profile: str,
        source_email: str,
        target_email: str,
        local_db_path: str = fileDir.parents[2].joinpath(
            'Get-Your',
            'getyour',
            'getyour',
            'db.sqlite3',
            ),
        interactive: bool = True,
        source_conn = None,
        target_conn = None,
    ) -> None:
    """
    Clone the specified user from the source to the target profile.

    Parameters
    ----------
    source_profile : str
        The profile name of the source. This uses coftc-cred-man to pull
        database parameters.
    target_profile : str
        The profile name of the target. This uses coftc-cred-man to pull
        database parameters.
    source_email : str
        The email address of the user to clone.
    target_email : str
        The selected email address of the cloned user. This must be unique in
        the target database.
    local_db_path : str, optional
        Path to the local (SQLite) database (only used if one or more of the
        selected environments is 'local'). The default is the location of the
        default db.sqlite3 local Django database, assuming ``Get-Your-utils``
        repo shares the same parent directory with ``Get-Your`` repo.
    interactive : bool, optional
        Flag whether to run in 'interactive mode'. Disabling this mode will
        disable user prompts (with prompt-specific defaults) and set the
        connections to keepalive.
        
        NOTE THAT THE PROFILES WILL NOT BE VERIFIED IF THERE ARE ACTIVE
        CONNECTIONS.
        
        The default is True.
    source_conn : <database connection>, optional
        Connection to use for the source database. The default is None, meaning
        the connection will be established in this function.
    target_conn : <database connection>, optional
        Connection to use for the target database. The default is None, meaning
        the connection will be established in this function.

    Raises
    ------
    TypeError
        Raised when more than one ID exists for a user (shouldn't be possible).

    Returns
    -------
    None

    """
    passwordClone = get_secret('PASSWORD_CLONE_ACCOUNT')
        
    ## Connect to the databases, using a different connection for 'local' env
    
    # If not interactive mode, use already-open connections if they exist.
    # NOTE THAT THE PROFILES WILL NOT BE VERIFIED IF THERE ARE ACTIVE CONNECTIONS
    
    srcConn = source_conn
    # If connection is not active, (re)define
    if not conn_info(srcConn)['is_active']:
        if source_profile.endswith('_local'):
            srcConn = sqlite3.connect(local_db_path)
        else:
            srcConn = GetFoco('', db_profile=source_profile).conn
    # Set the cursor regardless of prior connection
    srcCursor = srcConn.cursor()
    # Define if local based on the connection type
    srcLocal = True if conn_info(srcConn)['type']=='sqlite' else False
    srcEnv = conn_info(srcConn)['environment']

    targetConn = target_conn
    # If connection is not active, (re)define
    if not conn_info(targetConn)['is_active']:
        if target_profile.endswith('_local'):
            targetConn = sqlite3.connect(local_db_path)
        else:
            targetConn = GetFoco('', db_profile=target_profile).conn
    # Set the cursor regardless of prior connection
    targetCursor = targetConn.cursor()
    # Define if local based on the connection type
    targetLocal = True if conn_info(targetConn)['type']=='sqlite' else False
    targetEnv = conn_info(targetConn)['environment']
    
    try:
        ## Gather user id from source table
        if srcLocal:
            queryStr = "select id from app_user where lower(email)=?"
        else:
            queryStr = sql.SQL(
                "select {fd} from {tbl} where lower({idfd})=%s"
            ).format(
                fd=sql.SQL(', ').join(map(sql.Identifier, ['id'])),
                tbl=sql.Identifier('public', 'app_user'),
                idfd=sql.Identifier('email'),
            )
        srcCursor.execute(queryStr, (source_email.lower(),))
        userOut = [x[0] for x in srcCursor.fetchall()]
        if len(userOut)>1:
            raise TypeError("More than one id exists for this user")
        srcUserId = userOut[0]
        
        # Check if user exists in target database
        if targetLocal:
            queryStr = "select count(*) from app_user where id=?"
        else:
            queryStr = sql.SQL(
                "select count(*) from {tbl} where {idfd}=%s"
            ).format(
                tbl=sql.Identifier('public', 'app_user'),
                idfd=sql.Identifier('id'),
            )
        targetCursor.execute(queryStr, (srcUserId,))
        userExists = True if targetCursor.fetchone()[0]>0 else False
        
        # If the user exists in target, gather the email address to display
        if userExists:
            if targetLocal:
                queryStr = "select email from app_user where id=?"
            else:
                queryStr = sql.SQL(
                    "select {fd} from {tbl} where {idfd}=%s"
                ).format(
                    fd=sql.SQL(', ').join(map(sql.Identifier, ['email'])),
                    tbl=sql.Identifier('public', 'app_user'),
                    idfd=sql.Identifier('id'),
                )
            targetCursor.execute(queryStr, (srcUserId,))
            duplicateEmail = targetCursor.fetchone()[0]
            
            # The user cannot be deleted if the source and target are the same or if
            # the target is PROD; in these cases, a new ID will be created, else prompt
            # for overwrite if in interactive mode. Overwrite is the default
            # if interactive==False
            if srcEnv != targetEnv and targetEnv != 'prod' and (
                    not interactive or (
                        interactive and Confirm.ask(
                            f"User exists in '{targetEnv}' tables (under [green]{duplicateEmail}[/green]). Okay to overwrite? If [cyan]no[/cyan], a new user will be created.",
                        )
                    )
                ):
                targetUserId = srcUserId
            else:
                # If src==target or target=='prod' or overwrite is not authorized, set
                # user_id to None and spoof userExists to False to designate that the
                # source will not be deleted
                targetUserId = None
                userExists = False
                
        else:
            # If the user id doesn't exist, use the source, Luke
            targetUserId = srcUserId
            
        print(
            f"\nCloning user from '{srcEnv}' to '{targetEnv}'...\n"
        )
        
        ## Get the encrypted password of the target
        
        # Find a 'dev' connection; else create one from getfoco_dev just for this
        if target_profile.endswith('dev'):
            devCursor = targetCursor
            passwordConnStr = targetConn.get_dsn_parameters()['dbname']
        elif source_profile.endswith('dev'):
            devCursor = srcCursor
            passwordConnStr = srcConn.get_dsn_parameters()['dbname']
        else:
            passwordConnStr = 'getfoco_dev'
            devConn = GetFoco('', db_profile=passwordConnStr).conn
            devCursor = devConn.cursor()
            
        queryStr = sql.SQL(
            "select {fd} from {tbl} where {idfd}=%s"
        ).format(
            fd=sql.SQL(', ').join(map(sql.Identifier, ['password'])),
            tbl=sql.Identifier('public', 'app_user'),
            idfd=sql.Identifier('email'),
        )
        devCursor.execute(queryStr, (passwordClone,))
        targetPassword = devCursor.fetchone()[0]
        
        # Close the dev connection, if exists
        try:
            devConn.close()
        except:
            pass
        
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
        # This is redundant, as a safety that users are *not* removed from PROD
        if userExists and not target_profile.endswith('prod'):
            # Loop through tableList backward and delete
            for table in reversed(tableList):
                
                if table == 'app_user':
                    idField = 'id'
                else:
                    idField = 'user_id'
                
                if targetLocal:
                    queryStr = f"delete from {table} where {idField}=?"
                else:
                    queryStr = sql.SQL(
                        "delete from {tbl} where {idfd}=%s"
                    ).format(
                        tbl=sql.Identifier('public', table),
                        idfd=sql.Identifier(idField),
                    )
                targetCursor.execute(queryStr, (targetUserId,))
                
            # Commit all deletions
            targetConn.commit()
        
        # Copy user from source to target databases
        for table in tableList:
            
            if srcLocal:
                queryStr = f"select * from {table}"
                srcCursor.execute(queryStr)
                fieldList = [x[0] for x in srcCursor.description if x[0]!='id']
            else:
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
            if srcLocal:
                queryStr = "select {fd} from {tbl} where {idfd}=?".format(
                    fd=', '.join(fieldList),
                    tbl=table,
                    idfd=idField,
                )
            else:
                queryStr = sql.SQL(
                    "select {fd} from {tbl} where {idfd}=%s"
                ).format(
                    fd=sql.SQL(', ').join(map(sql.Identifier, fieldList)),
                    tbl=sql.Identifier('public', table),
                    idfd=sql.Identifier(idField),
                )
            srcCursor.execute(queryStr, (srcUserId,))
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
                dbOut[0][fieldList.index('email')] = target_email
                
                # Set to password value to the target password gathered above
                dbOut[0][fieldList.index('password')] = targetPassword
                
                # Change phone number to unused (to prevent notifications)
                dbOut[0][fieldList.index('phone_number')] = '+13035551234'
                
                # Update is_archived to True (to account for PROD targets)
                dbOut[0][fieldList.index('is_archived')] = True
                
            # For all other tables
            else:
                # If target user_id is different than source, update the dataset with
                # the target
                if targetUserId != srcUserId:
                    for iteridx in range(len(dbOut)):
                        dbOut[iteridx][fieldList.index('user_id')] = targetUserId
                
                # Ensure the matching address(es) exist and use the target IDs
                if table == 'app_address':
                    
                    # Should only be one record (that will be modified below)
                    if len(dbOut) > 1:
                        raise TypeError("There should only be one app_address record")
                    
                    for addtype in ['eligibility_address_id', 'mailing_address_id']:
                        # Gather address
                        if srcLocal:
                            queryStr = "select address_sha1 from app_addressrd where id=?"
                        else:
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
                        if targetLocal:
                            queryStr = "select id from app_addressrd where address_sha1=?"
                        else:
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
                            if srcLocal:
                                queryStr = "select * from app_addressrd"
                                srcCursor.execute(queryStr)
                                addrFieldList = [x[0] for x in srcCursor.description if x[0]!='id']
                            else:
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
                            if srcLocal:
                                queryStr = "select {fd} from {tbl} where id=?".format(
                                    fd=', '.join(addrFieldList),
                                    tbl='app_addressrd',
                                )
                            else:
                                queryStr = sql.SQL(
                                    """select {fd} from {tbl} where "id"=%s"""
                                ).format(
                                    fd=sql.SQL(', ').join(map(sql.Identifier, addrFieldList)),
                                    tbl=sql.Identifier('public', 'app_addressrd'),
                                )
                            srcCursor.execute(
                                queryStr,
                                (dbOut[0][fieldList.index(addtype)], ),
                            )
                            srcAddrOut = list(srcCursor.fetchone())
                            
                            # Insert address into target DB and return the proper ID
                            if targetLocal:
                                queryStr = "insert into {tbl} ({fd}) VALUES ({vl})".format(
                                    fd=', '.join(addrFieldList),
                                    tbl='app_addressrd',
                                    vl=', '.join(['?']*len(addrFieldList)),
                                )
                                # Convert decimal.Decimal to Python-native
                                # types that SQLite can understand
                                srcAddrOut = [int(x) if isinstance(x, decimal.Decimal) and len(str(x).split('.'))==1 else float(x) if isinstance(x, decimal.Decimal) else x for x in srcAddrOut]
                            else:
                                queryStr = sql.SQL(
                                    "insert into {tbl} ({fd}) VALUES ({vl}) returning ID"
                                ).format(
                                    fd=sql.SQL(', ').join(map(sql.Identifier, addrFieldList)),
                                    tbl=sql.Identifier('public', 'app_addressrd'),
                                    vl=sql.SQL(', ').join(sql.Placeholder()*len(addrFieldList)),
                                )
                            targetCursor.execute(queryStr, srcAddrOut)
                            
                            if targetLocal:
                                targetAddrId = targetCursor.lastrowid
                            else:
                                targetAddrId = targetCursor.fetchone()[0]
                            
                            # Commit this insert so the foreign keys will behave
                            targetConn.commit()
                            
                        # Use the target ID instead of the source (regardless of the insert)
                        dbOut[0][fieldList.index(addtype)] = targetAddrId
            
            ## Insert into the target table
            
            # ID is the primary key and is ignored above, so must be added here UNLESS
            # a new user is being added (targetUserId is None)
            if idField == 'id' and targetUserId is not None:
                if targetLocal:
                    queryStr = "insert into {tbl} ({fd}) VALUES ({vl})".format(
                        fd=', '.join(fieldList+[idField]),
                        tbl=table,
                        vl=', '.join(['?']*(len(dbOut[0])+1)) # account for idField here
                    )
                    targetCursor.executemany(
                        queryStr,
                        # JSONify any dicts and convert back to list of tuples
                        # Also convert decimal.Decimal to Python-native types
                        # that SQLite can understand
                        [tuple([json.dumps(x) if isinstance(x, dict) else int(x) if isinstance(x, decimal.Decimal) and len(str(x).split('.'))==1 else float(x) if isinstance(x, decimal.Decimal) else x for idx,x in enumerate(elem)]+[targetUserId]) for elem in dbOut],
                    )
                else:
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
                        [tuple([json.dumps(x) if isinstance(x, dict) else x for idx,x in enumerate(elem)]+[targetUserId]) for elem in dbOut],
                    )
                
            else:
                if targetLocal:
                    queryStr = "insert into {tbl} ({fd}) VALUES ({vl})".format(
                        fd=', '.join(fieldList),
                        tbl=table,
                        vl=', '.join(['?']*len(dbOut[0]))
                    )
                    targetCursor.executemany(
                        queryStr,
                        # JSONify any dicts and convert back to list of tuples
                        # Also convert decimal.Decimal to Python-native types
                        # that SQLite can understand
                        [tuple([json.dumps(x) if isinstance(x, dict) else int(x) if isinstance(x, decimal.Decimal) and len(str(x).split('.'))==1 else float(x) if isinstance(x, decimal.Decimal) else x for idx,x in enumerate(elem)]) for elem in dbOut],
                    )
                    
                    # If this is a new user, get the new ID field
                    if idField == 'id':
                        targetUserId = targetCursor.lastrowid
                        
                else:
                    queryStr = sql.SQL(
                        """insert into {tbl} ({fd}) VALUES {vl}{rt}"""
                    ).format(
                        fd=sql.SQL(', ').join(map(sql.Identifier, fieldList)),
                        tbl=sql.Identifier('public', table),
                        vl=sql.SQL(', ').join(sql.Placeholder()*len(dbOut)),
                        # If this is a new user, get the new ID field
                        rt=sql.SQL(' RETURNING "id"') if idField=='id' else sql.SQL(''),
                    )
                    targetCursor.execute(
                        queryStr,
                        # JSONify any dicts and convert back to list of tuples
                        [tuple([json.dumps(x) if isinstance(x, dict) else x for idx,x in enumerate(elem)]) for elem in dbOut],
                    )
                
                # If this is a new user, get the new ID field
                if idField == 'id':
                    targetUserId = targetCursor.fetchone()[0]
        
    except:
        # Rollback connections (this will passthrough if no open transactions)
        srcConn.rollback()
        targetConn.rollback()
        raise
    
    else:
        # This commit is specifically for the final inserts
        targetConn.commit()
        
        print('User cloned!')
        print('ID: {}'.format(targetUserId))
        print("email: '{}'".format(target_email))
        print(
            "password: same as '{}' in '{}'".format(
                passwordClone,
                passwordConnStr,
            )
        )
        
    finally:
        # Close cursors (this will passthrough if already closed)
        srcCursor.close()
        targetCursor.close()
        
        # Only attempt to close if in interactive mode
        if interactive:
            try:
                srcConn.close()
            except:
                pass
            try:
                targetConn.close()
            except:
                pass


if __name__ == '__main__':

    ## Gather source and target databases
    # Specify the generic database profile. This will have '_prod' or '_dev'
    # appended to it based on the source and target environment selections
    genericProfile = 'getfoco'
    
    srcEnv = Prompt.ask(
        "Enter the source environment",
        choices=['prod', 'dev', 'local'],
        default='prod',
    )
    srcProfile = f"{genericProfile}_{srcEnv}"
    
    targetEnv = Prompt.ask(
        "Enter the target environment",
        choices=['prod', 'dev', 'local'],
        default='dev',
    )
    targetProfile = f"{genericProfile}_{targetEnv}"
        
    srcEmail = Prompt.ask("Enter the email address of the user to clone")
        
    targetEmail = Prompt.ask("Enter an email address for the cloned user (note that this email may be sent communications from the app)")
    
    run_clone(
        srcProfile,
        targetProfile,
        srcEmail,
        targetEmail,
    )
