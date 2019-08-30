from address_checker.Address_Parsers.address_parser import parser_final, make_string, hasNumbers
import re
import requests
import json

#Poboxes
pobox = ['bag', 'po box', 'box','p.o.box','general post office box','lb','lmb','mailbag','mail bag','locked bag','community mail bag','cmb','gpo box','gpo','p.o. box','po box','pmb','post mail box','mailservice','ms','m s','m.s.','roadside mail box','rmb','road mail box','r.m.b.','roadside delivery','roadside mail service','rmd','private bag','private mail box','pmb']

#Remove these elements
remove_list = ['no.','no','restaurant', 'nm.','numero','number','nb.','left','right', 'around','nearby','near','basement','lobby','ground','upper','uppr','upr','lgf','podium','rooftop','apartment','staircase','stair']

#Remove these elements plus their next element, which is a number
remove_list_with_number = ['floor','level','platform', 'storney','flr.','flr','fl.','fl','lev.','lev','levl.','levl','lvl.','lvl','pf.','pf']

#Floor numbers
floors = ['1th','2th','3th','4th','5th','6th','7th','8th','9th']

#Finish streetnames, where blacklist_street has the streetnames that can't be finished.
afkorting = {'strt':'street','str.': 'street','str':'street', 'rd.':'road','rd':'road'}
blacklist_street = []

#streets that end with a number
straat_eind_cijfer =[]
straat_eind_cijfer = [x.replace(' ','').lower() for x in straat_eind_cijfer]

#recognize streetnames
straateind=['str','road','rd','cross','beach', 'back','bank','bur','bra','block','bayou','way','alley','brae','brea','cent','cla', 'clif','conc','gap','garden','hang','haven','head','little','out','path','por','rap','right','left','spa','sub','trai','val']

#some countries have the number in front of the string
def number_in_front(input):
    split = input.split()
    if hasNumbers(split[0]) and hasNumbers(split[-1]):
        output = make_string(split[1:])
        output = output.replace(split[-1],split[0])
    else:
        output = input
    return output

def find_postcode(input):
    regex = '(([gG][iI][rR] {0,}0[aA]{2})|(([aA][sS][cC][nN]|[sS][tT][hH][lL]|[tT][dD][cC][uU]|[bB][bB][nN][dD]|[bB][iI][qQ][qQ]|[fF][iI][qQ][qQ]|[pP][cC][rR][nN]|[sS][iI][qQ][qQ]|[iT][kK][cC][aA]) {0,}1[zZ]{2})|((([a-pr-uwyzA-PR-UWYZ][a-hk-yxA-HK-XY]?[0-9][0-9]?)|(([a-pr-uwyzA-PR-UWYZ][0-9][a-hjkstuwA-HJKSTUW])|([a-pr-uwyzA-PR-UWYZ][a-hk-yA-HK-Y][0-9][abehmnprv-yABEHMNPRV-Y]))) {0,}[0-9][abd-hjlnp-uw-zABD-HJLNP-UW-Z]{2}))'
    try:
        postcode = re.search(f"{regex}", input).group()
    except:
        postcode = ''
    input = input.replace(postcode,'')
    return input.strip(), postcode


def parser(input):
    address, postcode = find_postcode(input)
    address = number_in_front(address)
    output = parser_final(address, pobox, remove_list, remove_list_with_number, floors, blacklist_street, afkorting,straat_eind_cijfer, straateind)
    output.update({'postcode':postcode})
    # print(output['streetname'])
    try:
        response = json.loads(requests.get(f"http://37.97.229.131:5000/libpostal/?address={input}").text)
        libpostal_road = response['road'].title()
        if output['streetname']!=libpostal_road:
            output.update({'streetname':libpostal_road})
    except:
        pass
    return output