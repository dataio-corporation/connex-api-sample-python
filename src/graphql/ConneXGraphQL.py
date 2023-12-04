"""
ConneX GraphQL Client sample code.

This script allows the user to issue ConneX GraphQL queries.

When successful, the script will issue the GraphQL query and output
the response data in the console.

This script requires that `gql` be installed within the Python
environment you are running this script in.
"""

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

# Initialize HTTP transport with ConneX GraphQL server in localhost.
# Replace 'localhost' with the IP address of the ConneX Server machine if
# the server is running in a different machine.
transport = RequestsHTTPTransport(url="http://localhost:5001/graphql")

# Initialize the GraphQL client, fetch the schema to validate queries
client = Client(transport=transport, fetch_schema_from_transport=True)

# Issue GraphQL query to ConneX server and wait for a response
def connex_gql_query(request_string):
    # print(request_string)
    query = gql(request_string)
    result = client.execute(query, parse_result=True)    
    # print(result)
    return result

# Query for all handlers in the system
def handlers_query(): 
    # Uses 'systems' query : "Look up all the known PSV systems connected to this instance of ConneX." 

    # In this example we are querying for all programmers in the system. 
    # The script outputs a comma separated list of all the programmers and 
    # their associated handlers. 
    handlers = connex_gql_query(
        """
        query { 
            systems {
                handlerId
                entity{
                  entityIdentifier
                  entityName
                }
                handlerType
                ipAddress
                hostName
                machineFactory
            }
        }
    """
    )          
    # Print header row
    print("handler.Id,handler.Name,handler.Identifier,handler.Type,handler.ipAddress,handler.hostName,handler.machineFactory")
    handlers_list = handlers['systems']
    for handler in handlers_list:
        # Print the handlers as a list of comma separated values
        print(str(handler['handlerId']) 
          + "," + handler['entity']['entityName'] 
          + "," + handler['entity']['entityIdentifier']
          + "," + handler['handlerType'] 
          + "," + handler['ipAddress']   
          + "," + handler['hostName']
          + "," + handler['machineFactory'])

# Query for all programmers in the system
def programmers_query(): 
    # Uses 'programmers' query : "Look up all the known programmers connected to this instance of ConneX." 

    # In this example we are querying for all programmers in the system. 
    # The script outputs a comma separated list of all the programmers and 
    # their associated handlers. 
    programmers = connex_gql_query(
        """
        query { 
            programmers {
                programmerId  
                entity {
                    entityName
                    entityIdentifier
                }
                programmerType
                ipAddress
	            handler {
                    entity {
                        entityName                
                    }
	            }
            }
        }
    """
    )          
    # Print header row
    print("programmer.Id,programmer.Name,programmer.Identifier,programmer.Type,programmer.ipAddress,handler.Name")
    programmers_list = programmers['programmers']
    for programmer in programmers_list:
        handler = programmer['handler']
        # Print the programmers as a list of comma separated values
        if handler == None:
            print(str(programmer['programmerId']) 
              + "," + programmer['entity']['entityName'] 
              + "," + programmer['entity']['entityIdentifier']
              + "," + programmer['programmerType'] 
              + "," + programmer['ipAddress'])
        else:
            print(str(programmer['programmerId']) 
              + "," + programmer['entity']['entityName'] 
              + "," + programmer['entity']['entityIdentifier']
              + "," + programmer['programmerType'] 
              + "," + programmer['ipAddress']              
              + "," + programmer['handler']['entity']['entityName'])
            
# Query for all adapters in the system 
def adapters_query():    
    # Uses 'adapters' query : "Look up all the known adapters connected to this instance of ConneX."

    # In this example we are querying for all adapters in the system. 
    # The script outputs a list of comma separated values of all the adapters and 
    # their associated programmers. 
    adapters = connex_gql_query(
        """
        query { 
            adapters {
                adapterKey
                adapterId
                entity{
                    entityIdentifier
                }
                programmer{
                    entity {
                        entityName
                    }
                }
            }
        }
    """
    )          
    # Print header row
    print("adapter.Id,adapter.Identifier,adapter.Type,programmer.Name")
    adapters_list = adapters['adapters'] 
    for adapter in adapters_list:          
        programmer = adapter['programmer']
        # Print the adapters as a list of comma separated values
        if programmer == None:
            print(str(adapter['adapterKey'])
              + "," + str(adapter['entity']['entityIdentifier']) 
              + "," + adapter['adapterId'])
        else:
            print(str(adapter['adapterKey'])
              + "," + str(adapter['entity']['entityIdentifier']) 
              + "," + adapter['adapterId']
              + "," + programmer['entity']['entityName'])              

# Query for latest statistics of all adapters in the system
def latest_statistics_all_adapters_query():    
    # Uses 'adapters' query : "Look up all the known adapters connected to this instance of ConneX."
    # Uses 'latestAdapterStatistics' query : "Get the latest metric entries for the specified adapter."

    # In this example we are querying for the latest statistics of all adapters in the system. 
    # The script outputs a list of comma separated values of all the adapters and their associated 
    # latest statistics.
    
    # First, issue a query to get the entityIdentifier of each adapter
    adapters = connex_gql_query(
        """
        query { 
            adapters {
                entity{
                    entityIdentifier
                }
            }
        }
    """
    )      
    # Print header row
    print("adapter.Identifier,adapter.Type,cleanCount,lifetimeActuationCount,lifetimeContinuityFailCount,lifetimeFailCount,lifetimePassCount,socketIndex,adapterState")
    adapters_list = adapters['adapters']       
    for adapter in adapters_list:
        # Next, query the latest statistics for each valid adapter
        if adapter['entity']['entityIdentifier'] != None:
            try:
                adapter_stats = connex_gql_query(
                    """
                    query {{
                        latestAdapterStatistics(
                            entityIdentifier: \"{}\"
                        )
                        {{
                            adapterId
                            cleanCount
                            lifetimeActuationCount
                            lifetimeContinuityFailCount
                            lifetimeFailCount
                            lifetimePassCount
                            socketIndex
                            adapterState
                        }}
                    }}
                """.format(adapter['entity']['entityIdentifier'])
                )
            except:
                # When adapter has no statistics set to None to display only the adapter identifier
                adapter_stats = {'latestAdapterStatistics':None}
                
            # Print the statistics as a list of comma separated values
            if adapter_stats['latestAdapterStatistics'] == None:
                print(adapter['entity']['entityIdentifier'])
            else:
                print(adapter['entity']['entityIdentifier']
                  + "," + str(adapter_stats['latestAdapterStatistics']['adapterId']) 
                  + "," + str(adapter_stats['latestAdapterStatistics']['cleanCount'])
                  + "," + str(adapter_stats['latestAdapterStatistics']['lifetimeActuationCount'])
                  + "," + str(adapter_stats['latestAdapterStatistics']['lifetimeContinuityFailCount'])
                  + "," + str(adapter_stats['latestAdapterStatistics']['lifetimeFailCount'])
                  + "," + str(adapter_stats['latestAdapterStatistics']['lifetimePassCount'])
                  + "," + str(adapter_stats['latestAdapterStatistics']['socketIndex'])
                  + "," + str(adapter_stats['latestAdapterStatistics']['adapterState']))
    
# Query all MQTT messages with topic "programmingcomplete"
def programmingcomplete_query():
    # Uses 'messages' query : "Get all MQTT messages using paging (maximum of 50 items per page)."

    # In this example we are querying all messages with topic "programmingcomplete". 
    # We check the 'hasNextPage' field to determine whether there are more messages
    # to get, since the maximum number of messages to read at once is 50.       
    keep_reading = True
    skip = 0
    while keep_reading:        
        messages = connex_gql_query(
            """
            query {{ 
                messages (take:50 skip:{}
                    where: {{ 
                        topic: {{ 
                            contains: "programmingcomplete"               
                        }} 
                    }} ) {{
                    totalCount
                    items {{
                        topic 
                        timestamp 
                        payloadAsString 
                    }}
                    pageInfo {{
                        hasNextPage
                    }}
                }}
            }}
        """.format(skip)
        )        
        # In first query, print total number of messages found
        if skip == 0:
            total_messages = messages['messages']['totalCount']
            print(f'Total "programmingcomplete" messages found: {total_messages}')            
        # Get list of messages and print one message per line
        messages_list = messages['messages']['items']         
        for message in messages_list:
            print(message['timestamp'] + " | " + message['topic'] + " | " + message['payloadAsString'])        
        # Check if there are more pages to read
        keep_reading = messages['messages']['pageInfo']['hasNextPage']
        skip = skip + 50

# Query all MQTT messages in the database
def allmessages_query():
    # Uses 'messages' query : "Get all MQTT messages using paging (maximum of 50 items per page)."

    # In this example we are querying all messages. We check the 'hasNextPage' field
    # to determine whether there are more messages to get, since the maximum number of
    # messages to read at once is 50.
    keep_reading = True
    skip = 0
    while keep_reading:        
        messages = connex_gql_query(
            """
            query {{
                messages (take:50 skip:{} 
                    order: {{
                        timestamp: ASC
                    }} ) {{
                    totalCount
                    items {{
                        topic 
                        timestamp 
                        payloadAsString 
                    }}
                    pageInfo {{
                        hasNextPage
                    }}
                }}
            }}
        """.format(skip)
        )        
        # In first query, print total number of messages found
        if skip == 0:
            pending_messages = messages['messages']['totalCount']
            print(f'Total messages found: {pending_messages}')        
        # Get list of messages and print one message per line
        messages_list = messages['messages']['items']         
        for message in messages_list:
            print(message['timestamp'] + " | " + message['topic'] + " | " + message['payloadAsString'])        
        # Check if there are more pages to read
        keep_reading = messages['messages']['pageInfo']['hasNextPage']
        skip = skip + 50
        
# main program
def main(): 
    # Uncomment the example you want to test    
    
    handlers_query()
    
    # programmers_query()  
    
    # adapters_query()

    # latest_statistics_all_adapters_query()
    
    # programmingcomplete_query()
    
    # allmessages_query()
    
# Script entry point
if __name__ == '__main__':
    main()
