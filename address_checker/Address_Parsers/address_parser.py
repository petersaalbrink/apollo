# -*- coding: utf-8 -*-

import string
import re


#Poboxes
postbussen = ['postbus','p.o.box','p.o. box','po box', 'post bus', 'vo box', 'pastbus', 'past bus']

#Remove these elements
remove_list = ['no.','no','restaurant', '7-eleven','nm.','numero','number','nb.','rechts','dhl','links','hinterhaus','hausnummer','1-hg','2-hg','3-hg','4-hg','5-hg','6-hg','7-hg','1-hoog','2-hoog','3-hoog','4-hoog','5-hoog','6-hoog','7-hoog','t/m', 'T/m', 'T/M', 't/M','hoog','Hoog','HOOG','huis','laag','beneden','boven']

#Remove these elements plus their next element, which is a number
remove_list_with_number = ['vloer', 'floor', 'hal', 'vloer','t.a.v.','afd.','verd.','etage','étage','bus','room','vrt','bte','tor','boite']

#Floor numbers
floors = ['1e','2e','3e','4e','5e','6e','7e','8e','9e', '1th','2th','3th','4th','5th','6th','7th','8th','9th']

#Finish streetnames, where blacklist_street has the streetnames that can't be finished.
afkorting = {'str.':'straat'}
blacklist_street = []

#streets that end with a number
straat_eind_cijfer = []

#recognize streetnames
straateind=['str','weg','väg','laan','ple','sing','kad','plan','bazar','Tänav','super','tie','улица','katu','bul','街','Tar','shar','hof','sor','gar','lane','baan','алея','alieja','ถนน','strada','utca','calle','бульвар','Булевард','vard','aleja','οδός','unter','kamp','park','haven','beach','bur','and','via','maan','man','aut','calle','gat']


def is_all_lowercase(a_str):
    a_str = " ".join(re.findall("[a-zA-Z]+", a_str))
    a_str = a_str.replace(' ', '')
    for c in a_str:
        if c in string.ascii_uppercase:
            return False
    return True


def is_all_uppercase(a_str):
    a_str = " ".join(re.findall("[a-zA-Z]+", a_str))
    a_str = a_str.replace(' ', '')
    for c in a_str:
        if c in string.ascii_lowercase:
            return False
    return True


def number_removal(input):
    numbers = ['No.', 'NO.', 'no.']
    for n in numbers:
        input = input.replace(n, '')
    return input


def str_int_split(item):
    match = re.match(r"([A-Z]+)([0-9]+)([A-Z]+)", item, re.I)
    if match:
        items = match.groups()
        return items
    else:
        match = re.match(r"([A-Z]+)([0-9]+)", item, re.I)
        if match:
            items = match.groups()
            return items
        else:
            match = re.match(r"([0-9]+)([A-Z]+)([0-9]+)([A-Z]+)", item, re.I)
            if match:
                items = match.groups()
                return items
            else:
                match = re.match(r"([0-9]+)([A-Z]+)([0-9]+)", item, re.I)
                if match:
                    items = match.groups()
                    return items
                else:
                    match = re.match(r"([0-9]+)([A-Z]+)", item, re.I)
                    if match:
                        items = match.groups()
                        return items


# def find_postcode(input):
#     input = input.split()
#     postcode = ''
#     try:
#         for i, el in enumerate(input):
#             if len(el) == 6 and el[0:4].isdigit() and hasNumbers(el[4:5]) == False:
#                 postcode = el
#                 input.remove(el)
#             elif len(el) == 4 and el[0:4].isdigit() and len(input[i + 1]) == 2 and input[i + 1].isalpha():
#                 postcode = el + input[i + 1]
#                 input.remove(el)
#                 input.remove(input[i])
#         if postcode == '':
#             if len(input[-1]) == 2 and input[-1].isalpha() and len(input[-2]) == 4 and input[-2].isdigit():
#                 postcode = input[-2] + input[-1]
#                 input.remove(input[-2])
#                 input.remove(input[-1])
#             elif len(input[1]) == 2 and input[1].isalpha() and len(input[0]) == 4 and input[0].isdigit():
#                 postcode = input[0] + input[1]
#                 input.remove(input[1])
#                 input.remove(input[0])
#     except:
#         pass
#     return make_string(input), postcode


def make_string(lijst):
    string = ''
    for i in lijst:
        string += ' ' + i
    return string.strip()


def clean(item):
    sign = '!#$%&()*+,:;?@[\\]^_`{|}~'
    for i in sign:
        item = item.replace(i, ' ').replace('  ', ' ')
    for i in string.punctuation:
        item = item.strip(i).strip(' ' + i).strip(i + ' ')
    if '--' in item:
        item = item.replace('--', '-')
    return item


def remove_double_spaces(item):
    while '  ' in item:
        item = item.replace('  ', ' ')
    return item


def remove_betweenbrackets(item):
    return re.sub(r'\([^)]*\)', '', item).strip()


def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)


def hasLetters(input):
    return any(char in string.ascii_letters for char in input)

def splitting_correct(input):
    input = input.split()
    output = []
    if len(input) == 1 and is_all_lowercase(make_string(input)) == False:
        if ',' in input[0]:
            return input[0].split(',')
        elif hasNumbers(input[0]):
            if '-' in input[0]:
                input = make_string(input[0].split('-')).split()
                output = input[0]
                for element in input[1:]:
                    if hasNumbers(element) and hasLetters(element):
                        el1 = re.search(r"[aA-zZ]+", element).group()
                        el2 = re.search(r"[0-9]+", element).group()
                        if el1.isalpha():
                            output = output + '-' + el1
                        if el2.isdigit():
                            output = output + ' ' + el2
                    elif element.isalpha():
                        output = output + '-' + element
                    elif element.isdigit():
                        output = output + ' ' + element
                    else:
                        output = output + element
                output = output.strip()
                try:
                    if output[-1].isdigit() and output[-2].isdigit():
                        newnum = output[-2] + '-' + output[-1]
                        del output[-1]
                        del output[-1]
                        output.append(newnum)
                        return output.split()
                    else:
                        return output.split()
                except:
                    return output.split()
            else:
                return input
    else:
        return input


def splitting_correct_extra(input):
    split = input.split()
    output = ''
    for i, e in enumerate(split):
        try:
            if e.isdigit() and len(split[i + 1]) == 1 and split[i + 1].isalpha():
                output += ' ' + e + split[i + 1]
                del split[i + 1]
            else:
                output += ' ' + e
        except:
            output += ' ' + e
            pass
    return output


def forgotten_space(input, straat_eind_cijfer):
    output = ''
    temp_input = ''
    flag=False
    for el in input.split():
        if el.lower() in straat_eind_cijfer:
            return input
        if '-' in el and hasNumbers(el):
            extra_el = el.split('-')[-1]
            if extra_el.isdigit() or extra_el.isalpha() and len(extra_el)<4:
                flag = True
        if is_all_uppercase(el) == False and is_all_lowercase(el) == False and hasPunctuation(el)==False:
            app = make_string(re.findall('[0-9][A-Z][^A-Z]', el))
            temp_input += ' ' + app
            temp_input = temp_input.strip()
            if app == '':
                temp_input += ' ' + make_string(re.findall('[A-Z][^A-Z]*', el))
        else:
            temp_input += ' ' + el
    input = temp_input.strip()
    if len(input.split()) == 1:
        if hasPunctuation(input) == False:
            try:
                if flag and ('-'+extra_el) not in input:
                    return splitting_correct_extra(make_string(str_int_split(input)))+'-'+extra_el
                else:
                    return splitting_correct_extra(make_string(str_int_split(input)))
            except:
                if flag and ('-'+extra_el) not in input:
                    return splitting_correct_extra(input)+'-'+extra_el
                else:
                    return splitting_correct_extra(input)
        else:
            for i in string.punctuation:
                input = input.replace(i, ' ')
            split = input.split()
            for element in split:
                try:
                    output += ' ' + make_string(str_int_split(element))
                except:
                    output += ' ' + element
            if flag and ('-'+extra_el) not in input:
                return splitting_correct_extra(output.strip())+'-'+extra_el
            else:
                return splitting_correct_extra(output.strip())
    else:
        inp_split = input.split()
        for element in inp_split:
            if hasNumbers(element) and hasLetters(element):
                try:
                    output += ' ' + make_string(str_int_split(element))
                except:
                    output += ' ' + element
            else:
                output += ' ' + element
        if flag and ('-'+extra_el) not in input:
            return splitting_correct_extra(output.strip())+'-'+extra_el
        else:
            return splitting_correct_extra(output.strip())
    if flag and ('-'+extra_el) not in input:
        return input+'-'+extra_el
    else:
        return input

def replace_ins(repl, inpl, input):
    insensitive = re.compile(re.escape(repl), re.IGNORECASE)
    return insensitive.sub(inpl, input)


def hasPunctuation(input):
    return any(char in string.punctuation for char in input)


def remove_email(input):
    input = input.split()
    for x in input:
        x = re.search(r"[^@]+@[^@]+\.[^@]+", x)
        if x != None:
            input.remove(x.group(0))
    return make_string(input)


def remove_adjacent(L):
    return [elem for i, elem in enumerate(L) if i == 0 or L[i - 1] != elem or hasNumbers(str(elem)) == False]


def take_out_punct(item):
    if hasPunctuation(item):
        if ' - ' in item:
            item = item.replace(' - ', '-')
        if ' -' in item:
            item = item.replace(' -', '-')
        if '- ' in item:
            item = item.replace('- ', '-')
        if ' / ' in item:
            item = item.replace(' / ', '/')
        if ' /' in item:
            item = item.replace(' /', '/')
        if '/ ' in item:
            item = item.replace('/ ', '/')
        item = remove_double_spaces(item)
        output = []
        split = item.split()
        for n, i in enumerate(split):
            if hasNumbers(i) and hasPunctuation(i):
                for sp in string.punctuation:
                    if sp in i:
                        output.append(i.replace(sp, ' '))
            else:
                output.append(i)
            if hasLetters(i) == False and n > 0:
                return make_string(output).replace('  ', ' ')
        return make_string(output).replace('  ', ' ')
    else:
        return item


def general_cleaning(input, straateind, straat_eind_cijfer):
    input = remove_email(input)
    if '(' and ')' in input:
        input = remove_betweenbrackets(input)

    if ',' in input:
        split = input.split(',')
        for s in split:
            for strt in straateind:
                if strt.lower() in s.lower() and hasNumbers(s):
                    input = s
                else:
                    pass

    split = input.split()
    for index, s in enumerate(split):
        if hasNumbers(s) and (s.lower() not in straat_eind_cijfer) and hasLetters(s) and ('/' not in s) and ('-' not in s):
            cd = 0
            cl = 0
            for l in s:
                if l.isdigit():
                    cd += 1
                if l.isalpha():
                    cl += 1
            if cl < 4 and cd>1 and cl>1:
                del split[index]
    input = make_string(split)

    input = forgotten_space(clean(remove_double_spaces(input)), straat_eind_cijfer)

    if ' - ' in input:
        input = input.replace(' - ', '-')
    if ' -' in input:
        input = input.replace(' -', '-')
    if '- ' in input:
        input = input.replace('- ', '-')
    if ' / ' in input:
        input = input.replace(' / ', '/')
    if ' /' in input:
        input = input.replace(' /', '/')
    if '/ ' in input:
        input = input.replace('/ ', '/')
    input = clean(make_string(remove_adjacent(splitting_correct(input)))).title()
    return number_removal(input)


def drop_triplicate_letters(oldstring):
    try:
        newstring = oldstring[0]
        for char in oldstring[1:]:
            try:
                if char == newstring[-1] and char == newstring[-2] and char == newstring[-3] and char.isalpha():
                    pass
                else:
                    newstring += char
            except:
                newstring += char
                pass
        return newstring
    except:
        return oldstring


def cleaner_postbus_checker(input, postbussen, straateind, straat_eind_cijfer):
    postbusnum = ''
    for pb in postbussen:
        if pb.lower() in input.lower():
            m = re.search(f'{pb} (\d+)', input, re.IGNORECASE)
            if m is not None:
                postbusnum = m.group(1)
                input = re.sub(f'{pb} {postbusnum}', '', input.lower(), re.IGNORECASE)
            else:
                m = re.search(f'{pb}(\d+)', input, re.IGNORECASE)
                if m is not None:
                    postbusnum = m.group(1)
                    input = re.sub(f'{pb}{postbusnum}', '', input.lower(), re.IGNORECASE)
                else:
                    input = re.sub(f'{pb}', '', input.lower(), re.IGNORECASE)
    return {'cleaned_up': drop_triplicate_letters(general_cleaning(input, straateind,  straat_eind_cijfer)).strip(), 'pobox': postbusnum}

def clean_street(item, afkorting, blacklist_street):
    sign = '#$%&()*+,:;?@[\\]^_`{|}~'
    for i in sign:
        item = item.replace(i, ' ').replace('  ', ' ')
    for i in string.punctuation:
        item = item.strip(i).strip(' ' + i).strip(i + ' ')
    split = item.split()
    le = split[-1]
    if hasNumbers(le) or len(le)==1:
        item = item.split(le)[0]

    # for element in split[1:]:
    #         # if len(element)==1:
    #         #     pass
    #         # else: output+=' '+element
    #     if hasNumbers(element):
    #         return afkorting_to_full(item.split(element)[0].strip())
    # except:
    output = item
    return afkorting_to_full(output.strip(), blacklist_street, afkorting)

def finding_another_hn(item, straateind):
    split = item.split()
    for le in split[::-1][0:(len(split)-1)]:
        for strt in straateind:
            if strt.lower() in le.lower():
                return '',''
        if le.isdigit():
            return le, ''
        elif hasNumbers(le):
            if hasPunctuation(le):
                if '.' in le:
                    le = le.replace('.','')
                if '-' in le:
                    lespl = le.split('-')
                    if lespl[0].isdigit():
                        return lespl[0], lespl[1]
                if '/' in le:
                    lespl = le.split('/')
                    if lespl[0].isdigit():
                        return lespl[0], lespl[1]
            output_hn = ''
            output_hne = ''
            for n, el in enumerate(le):
                if el.isdigit():
                   output_hn+= el
                elif el.isalpha():
                    for el in le[n:]:
                        if el.isalpha():
                            output_hne+= el
                        else:
                            return output_hn, output_hne
            if output_hn!='':
                return output_hn, output_hne
    return '',''

def make_name(name):
    return make_string([make_title(i) for i in name.split()])


def make_title(word):
    if word.strip() == "'T":
        return "'t"
    elif word[0].isdigit() or word[0] == "'" or word[0] == '"':
        return word
    else:
        return word.title()


def afkorting_to_full(street, blacklist_street, afkorting):
    if street not in blacklist_street:
        for element in list(afkorting.keys()):
            if element in street.lower():
                rex = re.compile(f'.*{element.lower()}$', flags=re.IGNORECASE)
                if rex.search(street) != None:
                    street = replace_ins(element + ' ', afkorting[element], street + ' ')
    return afkortingen_fix(street)


def afkortingen_fix(input):
    if 'pres ' in input.lower():
        input = replace_ins('pres', 'President ', input)
    if 'kon.' in input.lower():
        input = replace_ins('kon.', 'Koning ', input)
    if 'pres.' in input.lower():
        input = replace_ins('pres.', 'President ', input)
    return input.replace('  ', ' ')


def remove_shit(input):
    for p in '!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~':
        if p in input:
            input = input.replace(p, '')
    return input.strip()


def clean_ext(input):
    for el in string.punctuation:
        if el in input:
            input = input.replace(el, '')
    if len(input)>2 and input.isalpha():
        input = ''
    elif len(input)>3 and input.isdigit():
        input = ''
    elif len(input)>3 and hasLetters(input):
        input = ''
    return input

def parser(address, postbussen, remove_list, remove_list_with_number, floors, blacklist_street, afkorting, straat_eind_cijfer, straateind):
    output= {'streetname': '', 'housenumber': '', 'double_hn': '', 'housenumber_ext': '', 'postcode': '', 'pobox':'','extra':''}
    try:
        if address == None or address=='':
            return output

        huisnum = ''
        huisnum_ext = ''
        street = ''
        postcode = ''
        blacklist_used = False
        double_hn = False

        #look for hidden postcode.
        address = remove_email(address)


        postbus_check = cleaner_postbus_checker(address, postbussen, straateind, straat_eind_cijfer)
        address = postbus_check['cleaned_up']
        postbus = postbus_check['pobox']

                #Checking if there are numbers in the addresses, which could be a housenumber or part of the streetname.
        if hasNumbers(address):
            split = address.split()
            for i, element in enumerate(split):
                if element.lower() in remove_list_with_number:
                    del split[i]
                    del split[i]

                elif len(element) > 4 and element.isdigit():
                    if int(element[0:4]) not in range(1940,1945):
                        del split[i]

                try:
                    if element.lower() in floors and split[i+1].lower() in ['et','etage','etag','et.','etag.','verdieping']:
                        del split[i]
                        del split[i]
                except:
                    pass

                if element.lower() in remove_list:
                    del split[i]
            if '-' in split[-1]:
                try:
                    special_check = str_int_split(split[-1].split('-')[-1])
                    if len(special_check) == 3 and len(special_check[0]) == 1 and len(special_check[1]) == 2 and len(
                            special_check[2]) == 2 and special_check[0].isalpha() and special_check[1].isdigit() and \
                            special_check[2].isalpha():
                        address = clean(make_string(split[0:-1]+split[-1].split('-')[0:-1]))

                        split = address.split()
                except:
                    pass

            #remove nonsense around elements.
            for i in '#$%&()!*+,:;?@[\\]^_`{|}~':
                split = [x.strip(i).strip(' ' + i).strip(i + ' ') for x in split]

            #housenumber and address name are swapped we should swap back before performing parsing.
            if hasNumbers(split[-1])== False and split[0].isdigit():
                address = make_string(split[1:]+[split[0]])
                split = address.split()
            elif hasNumbers(split[0]) and split[0][0].isdigit() and split[-1][0].isdigit()== False and split[0] not in ['1e','2e','3e','4e']:
                address = make_string(split[1:]+[split[0]])
                split = address.split()

            #le is last element, which we check if it is a number or not.
            #le2 is second last element.
            le = split[-1]
            try: le2 = split[-2]
            except: le2 = ''

            if le.isdigit():

                if len(le)<8 and len(le)>4 and le.isdigit() and int(le[0:4]) in list(range(1940,1946)):
                    huisnum = le[4:]
                    street = make_string(split[0:-1])+' '+le[0:4]
                    return {'streetname': clean(make_name(street)), 'housenumber': huisnum, 'double_hn': double_hn,
                            'housenumber_ext': '', 'postcode': postcode.upper(),'pobox': postbus}

                elif len(split) == 1 and len(str(split[0])) < 5:
                    huisnum = le
                    street = ''
                    huisnum_ext = ''

                # Check if second last element is digit this would give an extension number or maybe a streetname with a number in it. These special streets are given in blacklist_streetwithnum.
                elif '-' in le2 and hasNumbers(le2.split('-')[-1]):
                    if split[-3].isdigit()==False and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'') in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le
                        street = make_string(split[0:-1])
                        huisnum_ext = ''
                    elif split[-3].isdigit() and make_string(split[0:-3]).replace(' ','').replace(le2.split('-')[-1],'') in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le2
                        street = make_string(split[0:-2])
                        huisnum_ext = le
                    elif split[-2].isdigit() and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'') in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le
                        street = make_string(split[0:-2])
                        huisnum_ext = ''
                    else:
                        huisnum = le2
                        street = make_string(split[0:-2])
                        huisnum_ext = le

                elif le2.isdigit():
                    if split[-3].isdigit()==False and make_string(split[0:-2]).replace(' ','').lower() in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le
                        street = make_string(split[0:-1])
                        huisnum_ext = ''
                    elif split[-3].isdigit() and make_string(split[0:-3]).replace(' ','').lower() in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le2
                        street = make_string(split[0:-2])
                        huisnum_ext = le
                    elif split[-2].isdigit() and make_string(split[0:-2]).replace(' ','').lower() in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le
                        street = make_string(split[0:-2])
                        huisnum_ext = ''
                    else:
                        huisnum = le2
                        street = make_string(split[0:-2])
                        huisnum_ext = le

                elif split[0].isdigit() and split[-2].isdigit() == False and make_string(split[1:-1]).replace(' ','').lower() in straat_eind_cijfer:

                    blacklist_used = True
                    huisnum = split[0]
                    street = make_string(split[1:])
                    huisnum_ext = ''
                elif split[0].isdigit() and split[-2].isdigit() and make_string(split[1:-2]).replace(' ','').lower() in straat_eind_cijfer:
                    blacklist_used = True
                    huisnum = split[0]
                    street = make_string(split[1:-1])
                    huisnum_ext = ''
                elif split[0].isdigit() and split[-1].isdigit() and make_string(split[1:-1]).replace(' ','').lower() in straat_eind_cijfer:
                    blacklist_used = True
                    huisnum = split[0]
                    street = make_string(split[1:])
                    huisnum_ext = ''


                # If second last element is not a digit we assume that there is no extension.
                else:
                    huisnum = le
                    street = make_string(split[0:-1])
                    huisnum_ext = ''

            #if there are no numbers in the last element this is probably an extension because we already removed postcodes.
            elif le.isalpha():
                huisnum_ext = le

                #if split has only one element. It means there is only a streetname given.
                if len(split)==1:
                    return {'streetname': make_name(afkorting_to_full(split[0], blacklist_street, afkorting)), 'housenumber': '', 'double_hn': double_hn, 'housenumber_ext':'', 'postcode': postcode.upper(), 'pobox': postbus}

                #if the second last element is a digit. It is likely that this is the house number and the last element an extension.
                if le2.isdigit():
                    huisnum = le2
                    street = make_string(split[0:-2])

                #if le2 is not an entire digit, we split the element on digits and letters and assume that le is trash element,
                # such that the number in le2 is the housenumber and extension together.
                else:
                    hn_h = str_int_split(le2)
                    try:
                        if len(hn_h) > 2 and hn_h[0].isdigit() and hn_h[2].isdigit():
                            huisnum = hn_h[0]
                            huisnum_ext = hn_h[1] + hn_h[2]
                            street = make_string(split[0:-2])
                        elif hn_h[0].isdigit() == True:
                            huisnum = hn_h[0]
                            huisnum_ext = hn_h[1]
                            street = make_string(split[0:-2])
                        elif hasNumbers(hn_h[0])==False and len(split)>2:
                            if split[-3].isdigit() == True:
                                huisnum = le2
                                huisnum_ext = le
                                street = make_string(split[0:-3])
                    except:
                        pass

            #if there is a dash in the last element, we should investigate this.
            #there is probably a split between housenumber and extension given with this dash.
            elif '-' in le:
                lespl = list(map(remove_shit, le.split('-')))
                #check out the blacklist for streets because some might have a dash. We try to split and check in the right way.
                # It is an extensive part of the code for actually a small number of possible cases. But also people on these streets want to receive their post ;-)

                if make_string(split[0:-1]+[lespl[0]]+['-']).replace(' ','').lower() in straat_eind_cijfer:
                    blacklist_used = True
                    le_dash_spl = lespl[-1].split()
                    if len(le_dash_spl)==1:
                        street = address
                        huisnum = ''
                        huisnum_ext = ''
                    else:
                        if le_dash_spl[-1].isdigit():
                            huisnum = le_dash_spl[-1]
                            huisnum_ext = ''
                            street = clean(address.split(huisnum)[0])
                        elif le_dash_spl[-1].isalpha() and len(le_dash_spl[-1])<3:
                            if le_dash_spl[-2].isdigit():
                                huisnum = le_dash_spl[-2]
                                huisnum_ext = le_dash_spl[-1]
                                street = clean(address.split(huisnum)[0])
                            else:
                                return {'streetname': clean_street(make_name(address), afkorting, blacklist_street), 'housenumber': '', 'double_hn': double_hn, 'housenumber_ext': '', 'postcode': postcode.upper(), 'pobox': postbus}
                        elif hasNumbers(le_dash_spl[-1]):
                            try:
                                hn_h = str_int_split(le_dash_spl[-1])
                                if len(hn_h) > 2 and hn_h[0].isdigit() and hn_h[2].isdigit():
                                    huisnum = hn_h[0]
                                    huisnum_ext = hn_h[1] + hn_h[2]
                                    street = clean(address.split(huisnum)[0])
                                elif hn_h[0].isdigit():
                                    huisnum = hn_h[0]
                                    huisnum_ext = hn_h[1]
                                    street = clean(address.split(huisnum)[0])
                                elif hasNumbers(hn_h[0]) == False and len(split)>2:
                                    if split[-3].isdigit():
                                        huisnum = le_dash_spl[-1]
                                        huisnum_ext = ''
                                        street = clean(address.split(huisnum)[0])
                            except:
                                pass
                        else:
                            return {'streetname': clean_street(make_name(address), afkorting, blacklist_street), 'housenumber': '', 'double_hn': double_hn, 'housenumber_ext': '',
                                    'postcode': postcode.upper(), 'pobox': postbus}

                elif make_string(split[0:-1]).replace(' ','').lower() in straat_eind_cijfer:
                    huisnum = le.split('-')[0]
                    huisnum_ext = le.split('-')[1]
                    street = make_string(split[0:-1])
                    if huisnum_ext.isdigit():
                        if int(huisnum)<int(huisnum_ext):
                            double_hn = True
                    return {'streetname': clean_street(make_name(street), afkorting, blacklist_street),
                            'housenumber': remove_shit(str(int(huisnum))), 'double_hn': double_hn,
                            'housenumber_ext': clean_ext(remove_shit(huisnum_ext)), 'postcode': postcode.upper(),
                            'pobox': postbus}

                elif hasNumbers(split[0]) and split[0].replace(' ','').lower() in straat_eind_cijfer and le.split('-')[0].isdigit():
                    huisnum = le.split('-')[0]
                    huisnum_ext = le.split('-')[1]
                    street = split[0]
                    if huisnum_ext.isdigit():
                        if int(huisnum)<int(huisnum_ext):
                            double_hn = True
                    return {'streetname': clean_street(make_name(street), afkorting, blacklist_street),
                            'housenumber': remove_shit(str(int(huisnum))), 'double_hn': double_hn,
                            'housenumber_ext': clean_ext(remove_shit(huisnum_ext)), 'postcode': postcode.upper(),
                            'pobox': postbus}



                #if the last element is a letter, we can assume that it is an extension number if the element before that is also a number.
                elif lespl[-1].isalpha():
                    huisnum_ext = lespl[-1]

                    #check if lespl first element is a digit, such that we have the housenumber.
                    if lespl[0].isdigit():
                        huisnum = lespl[0]
                        street = make_string(split[0:-1])

                    #if lespl first element is not a digit, but a letter we dont have a housenumber and extension split with a dash, but maybe some trash element or the whole element lespl is an extension.
                    elif lespl[0].isalpha():

                        #check now the second last element, if this is a digit, we have the housenumber and take the whole last element as extension.
                        if le2.isdigit():
                            huisnum = le2
                            huisnum_ext = le
                            street = make_string(split[0:-1])

                        #check if second last element has a number if it does, we split in number and digit as we did before.
                        elif hasNumbers(le2):
                            try:
                                hn_h = str_int_split(le2)
                                if len(hn_h) > 2 and hn_h[0].isdigit() and hn_h[2].isdigit():
                                    huisnum = hn_h[0]
                                    huisnum_ext = hn_h[1] + hn_h[2]
                                    street = make_string(split[0:-2])
                                elif hn_h[0].isdigit() == True:
                                    huisnum = hn_h[0]
                                    huisnum_ext = hn_h[1]
                                    street = make_string(split[0:-2])
                                elif hasNumbers(hn_h[0]) == False and len(split)>2:
                                    if split[-3].isdigit() == True:
                                        huisnum = le2
                                        huisnum_ext = ''
                                        street = make_string(split[0:-3])
                            except:
                                pass

                    #if last element has no digit or letter as first element, there is something wrong because we already cleaned for punctuations. So, give back everything.
                    else:
                        hn_h = str_int_split(lespl[0])
                        if hn_h[0].isdigit() and hn_h[1].isalpha():
                            huisnum_ext = hn_h[1]
                            huisnum = hn_h[0]
                            street = make_string(split[0:-1])
                        elif hn_h[0].isalpha():
                            street = make_string(split[0:-1])
                            huisnum = ''
                            huisnum_ext = ''

                #if the last element of the dash-splitted elements is a digit, this might be an extension number if the element before is also a number.
                elif lespl[-1].isdigit():
                    if lespl[0].isdigit():
                        # if int(lespl[0])<int(lespl[1]) and int(lespl[1])>9:
                        #     huisnum= lespl[0]
                        #     huisnum_ext = ''
                        #     street = make_string(split[0:-1])
                        if int(lespl[0]) < int(lespl[1]):
                            huisnum = lespl[0]
                            huisnum_ext= lespl[1]
                            double_hn = True
                            street = make_string(split[0:-1])
                        elif int(lespl[0]) > int(lespl[1]):
                            huisnum = lespl[0]
                            huisnum_ext = lespl[1]
                            street = make_string(split[0:-1])
                        elif int(lespl[0]) == int(lespl[1]) and int(lespl[0])<9:
                            huisnum = lespl[0]
                            huisnum_ext = lespl[1]
                            street = make_string(split[0:-1])
                        elif int(lespl[0]) == int(lespl[1]) and int(lespl[0])>=9:
                            huisnum = lespl[0]
                            huisnum_ext = ''
                            street = make_string(split[0:-1])

                    #maybe the dash is used for splitting housenumber and streetname
                    else:
                        huisnum = lespl[-1]
                        street = clean(address.split(huisnum)[0])
                elif hasNumbers(lespl[-1]):
                    if lespl[-2].isdigit():
                        huisnum = lespl[-2]
                        huisnum_ext = lespl[-1]
                        if len(huisnum_ext)>4:
                            huisnum_ext = ''
                        street = clean(address.split(huisnum)[0])
                    elif hasLetters(lespl[-2]) and hasNumbers(lespl[-2]):
                        sis = str_int_split(lespl[-2])
                        if sis[0].isdigit():
                            huisnum = sis[0]
                            street = clean(address.split(huisnum)[0])
                        if sis[1].isalpha():
                            huisnum_ext = sis[1]
                        if huisnum == '':
                            sis = str_int_split(lespl[-2])
                            if sis[0].isdigit():
                                huisnum = sis[0]
                                street = clean(address.split(huisnum)[0])
                            if sis[1].isalpha():
                                huisnum_ext = sis[1]

                #if still not found, we might have an issue and give back everything as street
                else:
                    return {'streetname': clean_street(make_name(address), afkorting, blacklist_street), 'housenumber': '', 'double_hn': double_hn, 'housenumber_ext': '','postcode': postcode.upper(), 'pobox': postbus}

            #we do the same for backslash as for dashes.
            elif '/' in le:
                lespl = list(map(remove_shit, le.split('/')))

                # if the last element is a letter, we can assume that it is an extension number if the element before that is also a number.
                if lespl[-1].isalpha():
                    huisnum_ext = lespl[-1]

                    # check if lespl first element is a digit, such that we have the housenumber.
                    if lespl[0].isdigit():
                        huisnum = lespl[0]
                        street = make_string(split[0:-1])

                    # if lespl first element is not a digit, but a letter we dont have a housenumber and extension split with a backslash, but maybe some trash element or the whole element lespl is an extension.
                    elif lespl[0].isalpha():

                        # check now the second last element, if this is a digit, we have the housenumber and take the whole last element as extension.
                        if le2.isdigit():
                            huisnum = le2
                            huisnum_ext = le
                            street = make_string(split[0:-1])

                        # check if second last element has a number if it does, we split in number and digit as we did before.
                        elif hasNumbers(le2):
                            try:
                                hn_h = str_int_split(le2)
                                if len(hn_h) > 2 and hn_h[0].isdigit() and hn_h[2].isdigit():
                                    huisnum = hn_h[0]
                                    huisnum_ext = hn_h[1] + hn_h[2]
                                    street = make_string(split[0:-2])
                                elif hn_h[0].isdigit() == True:
                                    huisnum = hn_h[0]
                                    huisnum_ext = hn_h[1]
                                    street = make_string(split[0:-2])
                                elif hasNumbers(hn_h[0]) == False and len(split) > 2:
                                    if split[-3].isdigit() == True:
                                        huisnum = le2
                                        huisnum_ext = ''
                                        street = make_string(split[0:-3])
                            except:
                                pass

                    # if last element has no digit or letter as first element, there is something wrong because we already cleaned for punctuations. So, give back everything.
                    else:
                        huisnum_ext = ''
                        huisnum = ''
                        street = make_string(split[0:-1])

                # if the last element of the backslash-splitted elements is a digit, this might be an extension number if the element before is also a number.
                elif lespl[-1].isdigit():
                    if lespl[0].isdigit():
                        # if int(lespl[0]) < int(lespl[1]) and int(lespl[1]) - int(lespl[0]) <= 4:
                        #     huisnum = lespl[0]
                        #     huisnum_ext = ''
                        #     street = make_string(split[0:-1])
                        if int(lespl[0]) < int(lespl[1]):
                            huisnum = lespl[0]
                            huisnum_ext = lespl[1]
                            street = make_string(split[0:-1])
                            double_hn = True
                        elif int(lespl[0]) > int(lespl[1]):
                            huisnum = lespl[0]
                            huisnum_ext = lespl[1]
                            street = make_string(split[0:-1])
                        elif int(lespl[0]) == int(lespl[1]) and int(lespl[0]) < 5:
                            huisnum = lespl[0]
                            huisnum_ext = lespl[1]
                            street = make_string(split[0:-1])
                        elif int(lespl[0]) == int(lespl[1]) and int(lespl[0]) >= 5:
                            huisnum = lespl[0]
                            huisnum_ext = ''
                            street = make_string(split[0:-1])

                    # maybe the backslash is used for splitting housenumber and streetname
                    else:
                        huisnum = lespl[-1]
                        huisnum_ext = ''
                        street = clean(address.split(huisnum)[0])
                elif hasNumbers(lespl[-1]):
                    if lespl[-2].isdigit():
                        huisnum = lespl[-2]
                        huisnum_ext = lespl[-1]
                        if len(huisnum_ext)>4:
                            huisnum_ext = ''
                        street = clean(address.split(huisnum)[0])
                    elif hasLetters(lespl[-2]) and hasNumbers(lespl[-2]):
                        sis = str_int_split(lespl[-2])
                        if sis[0].isdigit():
                            huisnum = sis[0]
                            street = clean(address.split(huisnum)[0])
                        if sis[1].isalpha():
                            huisnum_ext = sis[1]
                        if huisnum == '':
                            sis = str_int_split(lespl[-2])
                            if sis[0].isdigit():
                                huisnum = sis[0]
                                street = clean(address.split(huisnum)[0])
                            if sis[1].isalpha():
                                huisnum_ext = sis[1]

                # if still not found, we might have an issue and give back everything as street
                else:
                    return {'streetname': clean_street(make_name(address), afkorting, blacklist_street), 'housenumber': '', 'double_hn': double_hn, 'housenumber_ext': '',
                            'postcode': postcode.upper(), 'pobox': postbus}

            # if digits and letters are mixed in the last element we check for this as well. Such that we split in the right way
            elif hasNumbers(le):
                hn_h = str_int_split(le)
                try:
                    if len(hn_h) > 2 and hn_h[0].isdigit() and hn_h[2].isdigit():
                        huisnum = hn_h[0]
                        huisnum_ext = hn_h[1] + hn_h[2]
                        street = make_string(split[0:-1])
                    elif len(hn_h) > 2 and hn_h[0].isalpha() and hn_h[-1].isalpha() and hn_h[-2].isdigit():
                        huisnum = hn_h[-2]
                        huisnum_ext = hn_h[-1]
                        street = clean(address.split(huisnum)[0])
                    elif hn_h[0].isdigit():
                        huisnum = hn_h[0]
                        huisnum_ext = hn_h[1]
                        street = make_string(split[0:-1])
                    # elif hn_h[0].isalpha() and len(split)>2:
                    #     if le2.isdigit():
                    #         huisnum = le2
                    #         huisnum_ext = le
                    #         street = make_string(split[0:-2])
                    elif hn_h[0].isalpha() and hn_h[-1].isdigit():
                        if hasNumbers(le2)!= True:
                            if len(hn_h[0]) < 3:
                                huisnum = hn_h[-1]
                                huisnum_ext = hn_h[0]
                                street = remove_shit(address.split(le)[0])
                            else:
                                huisnum = hn_h[-1]
                                street = remove_shit(address.split(huisnum)[0])
                                huisnum_ext = ''
                        elif le2.isdigit():
                                huisnum = le2
                                huisnum_ext = le
                                street = remove_shit(address.split(le2)[0])
                        elif hasNumbers(le2):
                            hn_h2 = str_int_split(le2)
                            if hn_h2[0].isdigit() and hn_h2[1].isalpha():
                                huisnum = hn_h2[0]
                                huisnum_ext = hn_h2[-1]
                                street = remove_shit(address.split(huisnum)[0])
                            else:
                                if len(hn_h[0]) < 3:
                                    huisnum = hn_h[-1]
                                    huisnum_ext = hn_h[0]
                                    street = remove_shit(address.split(le)[0])
                                else:
                                    huisnum = hn_h[-1]
                                    street = remove_shit(address.split(huisnum)[0])
                                    huisnum_ext = ''
                except: pass

                #check if there are streets in the blacklist with last element number+extension. If so, we need to mark 'blacklist_used' as True.
                if '-' in le2 and le2.split('-')[-1].isdigit():
                    if split[-3].isdigit()==False and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'').lower() in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-3].isdigit() and make_string(split[0:-3]).replace(' ','').replace(le2.split('-')[-1],'').lower() in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-2].isdigit() and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'').lower() in straat_eind_cijfer:
                        blacklist_used = True

                elif le2.isdigit():
                    if split[-3].isdigit()==False and make_string(split[0:-2]).replace(' ','').lower() in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-3].isdigit() and make_string(split[0:-3]).replace(' ','').lower() in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-2].isdigit() and make_string(split[0:-2]).replace(' ','').lower() in straat_eind_cijfer:
                        blacklist_used = True

        # when there are no numbers in the address whatsoever, we give back the whole address as streetname..
        else:
            # huisnum = ''
            # huisnum_ext = ''
            street = address
            return {'streetname': clean_street(make_name(street), afkorting, blacklist_street), 'housenumber': '', 'double_hn': double_hn,
                    'housenumber_ext':'', 'postcode': postcode.upper(), 'pobox': postbus}


        huisnum_ext=huisnum_ext.upper()
        #take out the leftover dashes in the extension
        if '-' in huisnum_ext:
            huisnum_ext = huisnum_ext.replace('-','')

        #if the length of the extension is longer than 2, we might have a wrong thing parsed. It should be filtered out already, but this is an extra back-up check.
        if clean_ext(huisnum_ext)!=huisnum_ext:
            huisnum_ext = clean_ext(huisnum_ext)

        if len(huisnum_ext)>4:
            huisnum_ext = ''
            # for item in straatnaamdeel:
            #     if item in huisnum_ext:
            #         street = huisnum_ext
            #         huisnum_ext = ''
        if huisnum_ext == huisnum and len(huisnum_ext)==3:
            huisnum_ext = ''

        try:
            if int(huisnum)<int(huisnum_ext):
                double_hn=True
        except: pass

        hn_2, hne_2 = finding_another_hn(clean(street), straateind)
        if hn_2 != '' and len(hn_2)<4 and blacklist_used==False:
            huisnum = hn_2
            huisnum_ext = hne_2

        street = street.split(huisnum)[0]

        if street =='':
            return {'streetname': '', 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': double_hn,
                    'housenumber_ext': remove_shit(huisnum_ext), 'postcode': postcode.upper(), 'pobox': postbus}
        else:
            if blacklist_used:
                return {'streetname': clean(make_name(street)), 'housenumber':remove_shit(str(int(huisnum))), 'double_hn': double_hn, 'housenumber_ext': clean_ext(remove_shit(huisnum_ext)), 'postcode': postcode.upper()}
            else:
                return {'streetname': clean_street(make_name(street), afkorting, blacklist_street), 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': double_hn,
                        'housenumber_ext': clean_ext(remove_shit(huisnum_ext)), 'postcode': postcode.upper(), 'pobox': postbus}

#     we doe a try/except. If something goes wrong, we have a plan b for parsing.
    except:

        #plan b: search for a housenumber. Everything before is street, all the rest is extension.
        try:
            split = address.split()
            for i, element in enumerate(split):
                if element.isdigit():
                    huisnum = element
                    street = make_string(split[0:i])
                    huisnum_ext = split[-1]
                    return {'streetname': clean_street(make_name(street), afkorting, blacklist_street), 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': double_hn,
                            'housenumber_ext': clean_ext(huisnum_ext), 'postcode': '', 'pobox': postbus}
        except:
            try:
                return {'streetname': clean_street(make_name(street), afkorting, blacklist_street), 'housenumber': '', 'double_hn': double_hn, 'housenumber_ext': '', 'postcode': postcode.upper(), 'pobox': postbus}
            except:
                return {'streetname': clean_street(make_name(address), afkorting, blacklist_street), 'housenumber': '', 'double_hn': double_hn, 'housenumber_ext': '', 'postcode': postcode.upper(), 'pobox': postbus}


def parser_final(input, postbussen=[], remove_list=[], remove_list_with_number=[], floors=floors, blacklist_street=[], afkorting={},straat_eind_cijfer=[], straateind=straateind):
    output = parser(input, postbussen, remove_list, remove_list_with_number, floors, blacklist_street, afkorting, straat_eind_cijfer, straateind)

    if output is None:
        # print(f'Had 2 tries.')
        output = parser(take_out_punct(input), postbussen, remove_list, remove_list_with_number, floors, blacklist_street, afkorting, straat_eind_cijfer, straateind)
    if output is None:
        # print(f'Hard to parse. Not sure if output is correct.')
        try:
            street = clean(clean_street(make_name(input), afkorting, blacklist_street))
            hn_2, hne_2 = finding_another_hn(input, straateind)
            street = street.split(hn_2+hne_2)[0]
            output={'streetname': street, 'housenumber': hn_2, 'double_hn': False, 'housenumber_ext': clean_ext(hne_2), 'postcode': '', 'extra': '','pobox': ''}
        except:
            try:
                street = clean(clean_street(make_name(input), afkorting, blacklist_street))
                hn_2, hne_2 = finding_another_hn(input, straateind)
                street = street.split(hn_2 + hne_2)[0]
                output = {'streetname': street, 'housenumber': hn_2, 'double_hn': False, 'housenumber_ext': clean_ext(hne_2), 'postcode': '', 'extra': '','pobox': ''}
            except:
                output = {'streetname': input, 'housenumber': '', 'double_hn': False, 'housenumber_ext': '',
                          'postcode': '', 'extra': '', 'pobox': ''}
    return output
