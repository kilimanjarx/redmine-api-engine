import json
import requests
from enum import Enum
from abc import ABC, abstractmethod

class BaseEngine(ABC):
    def __init__(self) -> None:
        self.base_url = ""

    @abstractmethod
    def connect(self): # this method must be override in subclass 
        pass

    def disconnect(self):
        pass

class RedmineConstant(Enum):
    """
    | declaration for constants to consume redmine api
    | enum type is not mutable 
    """
    REDMINE_URL = "YOUR-REDMINE-URL"
    ALL_CONTACTS = "SELECT employee_id, name FROM management_employee WHERE employee_id >"

class RedmineEngine(BaseEngine):  
    """
    | Initialise redmine connection for crud actions
    | consume redmine api object
    """      
    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.base_url = RedmineConstant.REDMINE_URL.value 
        self.contacts_query = RedmineConstant.ALL_CONTACTS.value
        self.last_employee = open('last_employee.txt', 'r').read()
        if kwargs.get('key') is not None: # get api key
            self.key = kwargs.get('key')
        self.connect() # dry run to check if connection is authenticated and established

    def connect(self) -> int:
        """
        | make dry request to check if api key is valid
        | return status code
        """
        try:
            resp = requests.get(self.base_url + '/issues.json',  headers={'X-Redmine-API-Key':self.key})
            return resp.status_code
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err + "\n Failed during dry run!")

    def get_all_contacts(self) -> json: # get 
        """ 
        | get all contacts from redmine-it
        | paginated to 25 contacts
        | return json object
        """
        try:
            print("fetching all contacts...")
            resp = requests.get(self.base_url + '/contacts.json', headers={'X-Redmine-API-Key':self.key})
            return resp.json()
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)
    
    def update_all_contacts(self) -> int: # post
        """
        | inserting new contacts based on master data
        | return status code
        """
        master_list = self.get_master_list(query="{}".format(self.contacts_query))
        cleaned_list = self.contacts_formatter(master_list)
        for payload in cleaned_list:
            try:
                print("inserting new contacts...")
                resp = requests.post(self.base_url + '/contacts.json', headers={'Content-Type': 'application/json;charset=utf-8', 'X-Redmine-API-Key':self.key},json=payload)
                print("finished inserting new employee...")
                return resp.status_code
            except requests.exceptions.HTTPError as err:
                raise SystemExit(err)

    def get_master_list(self,**query) -> list:
        """
        | collecting raw list from master management_employee table
        | query employee after the employee id in last_employee.txt
        | return list of dictionary containing employee id and name
        """
        print("fething all employees after id {} from master...".format(self.last_employee))
        import secrets
        if query.get('query') is not None: # get api key
            query = query.get('query')
            conn = secrets.master.conn()
            cur = conn.cursor()
            cur.execute("""{}""".format(query + self.last_employee))
            rows = [ {'id':item[0], 'name': item[1]} for item in cur.fetchall() ]
            if not rows:
                raise SystemExit('No new employee! ')
            return rows
        else:
            raise SystemExit('Please supply query string!')

    def contacts_formatter(self,list) -> list:
        """
        | formatting raw master data into redmine contacts standard
        | get the last employee id and write to last_employee.txt
        | return a list 
        """
        print("preparing contacts list...")
        cleaned_list = []
        for x in list:
            cleaned_list.append(
                {
                    'contact':
                        {
                            "project_id": 3,
                            "first_name": x['name'],
                            "last_name": x['id']
                        }
                })
        sortg = sorted(list, key=lambda d: d['id'])[-1] # get the last element
        file1 = open('last_employee.txt', 'w')
        file1.write( str(sortg['id'] ))
        file1.close()
        return cleaned_list

redmine = RedmineEngine(key="")
redmine.update_all_contacts()

