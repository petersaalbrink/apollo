import re
import string

blacklist_street = ['Abraham van der Hulstln', 'AF de Savornin Lohmanstr', 'Albert vd Heijdenpln', 'Baron van der Heijdenln', 'Bartholomeus vd Helststr', 'Borgercompagniesterstr', 'Burg A P vd Blinkln', 'Burg Everwijn Langepln', 'Burg H v Sleenstr', 'Burg Janssen v Sonln', 'Burg Kronenburgln', 'Burg Schimmelpenninckstr', 'Burg Stronkhorststr', 'Burg ten Veldestr', 'Burg. Jos. Duijsensstr', 'Burgemeester Albertsstr', 'Burgemeester Aquariusstr', 'Burgemeester Bartelsstr', 'Burgemeester Beeckmanstr', 'Burgemeester Branderstr', 'Burgemeester Eikemastr', 'Burgemeester Galleestr', 'Burgemeester Gubbelsstr', 'Burgemeester Haffmansstr', 'Burgemeester Heersinkstr', 'Burgemeester Hoebersstr', 'Burgemeester Honijkstr', 'Burgemeester Huybenstr', 'Burgemeester Jonkerenstr', 'Burgemeester Kirkelsstr', 'Burgemeester Lambertstr', 'Burgemeester Linssenstr', 'Burgemeester MagnÃ©estr', 'Burgemeester Mertensstr', 'Burgemeester Peetersstr', 'Burgemeester Petersstr', 'Burgemeester Rietveldstr', 'Burgemeester Rijpstrastr', 'Burgemeester Simonszstr', 'Burgemeester Smeetsstr', 'Burgemeester v Raaijstr', 'Burgemeester van Akenstr', 'Burgemeester van Hooffln', 'Burgemeester van Rietstr', 'Burgemeester vd Broekstr', 'Burgemeester vd Meulenln', 'Burgemeester Voetenstr', 'Burgemeester Vranckenstr', 'Cecilia Benninghastr', 'Comm d Vos v Steenwijkln', 'Constantijn Huijgensstr', 'Cornelis de Houtmanstr', 'Cort van der Lindenstr', 'DaniÃ«l Catterwijckstr', 'Deken B.J. van Miertstr', 'Dirk Loef van Hornestr', 'Doctor C.J.K. v Aalststr', 'Dokter K.L. vd Veenln', 'Dokter van de Veldestr', 'Dr B Bastiaansestr', 'Dr R v Oppenraaijstr', 'Dr.Jan Herman van Heekln', 'Florence Nightingalestr', 'Franklin D. Rooseveltln', 'Frater J.M. Reijndersstr', 'G Hendrik Breitnerln', 'G.J.A. van Marrewijkstr', 'Generaal van Daalenstr', 'Gerrit van der Veenstr', 'Gezina van der Molenstr', 'Graaf de Liedekerkestr', 'Graaf van Bronkhorststr', 'Groen van Prinstererstr', 'H Beckering Vinckersstr', 'H Roland Holstln', 'H. Goeman Borgesiusstr', 'Helmich van Doernickstr', 'Hendrik van Bramerenstr', 'Hendrik Willem Mesdagln', 'Henriette Roland Holstln', 'Herman van der Haarstr', 'Hertog Karel van Gelrewg', 'Huibert van der Hoekpln', 'Huijbert van Schadijckln', 'J M vd Lugt Melsertln', 'J v Oldenbarneveldtstr', 'J van Oldenbarneveldtstr', 'J.A. de Bree-Meijerstr', 'Jacob van Heemskerkstr', 'Jacob van Wassenaerstr', 'Jan Josephsz v Goyenstr', 'Jan van der Heijdenstr', 'Jan van Steenbergenstr', 'Jan Wouter Heijmanszstr', 'Jkvr CM v Asch v Wijckln', 'Johannis van Rijswijkstr', 'Jonkheer Van Pallandtln', 'Juliana van Stolbergstr', 'Kapelaan H.G.M. Koopmansstr', 'Kapelaan J.A. Heerenstr', 'Kiliaen van Rensselaerln', 'Kon Willem-Alexanderpln', 'Koningin Wilhelminastr', 'Laan van SchÃ¶ndeln', 'Laan van Westerkappeln', 'Maria v Reigersberchstr', 'Meester Christiaansstr', 'Monseigneur Kierkelspln', 'Mr Egter v Wissekerkepln', 'Mr Steenbergenstr', 'Mr.Johan Hora Siccamaln', 'Notaris L P vd Blinkln', 'Notaris Philip Houbenstr', 'O Lv Vrouw ter Duinenln', 'Past. van Vroonhovenstr', 'Pastoor van de Weijstr', 'Pastoor van der Zandtstr', 'Pastoor van Sonsbeeckstr', 'Pastoor W. Hellemonsstr', 'Pater van den Elsenstr', 'Philips van Wassenaerln', 'Piet van der Lindenstr', 'Pieter v Slingelandtstr', 'Pieter van Ginnekenstr', 'Pr Willem Alexanderpln', 'Pr Willem Alexanderstr', 'Pr Willem-Alexanderstr', 'Prins Willem Alexanderln', 'Prins Willem-Alexanderln', 'Prof Alberdingk Thijmstr', 'Prof. Titus Brandsmastr', 'Rogier van der Weydenstr', 'Rogier vd Weijdenstr', 'Ruys de Beerenbrouckstr', 'Secretaris L. Jansenstr', 'Sir Winston Churchillln', 'Slotemaker de BruÃ¯nestr', "Storm v 's Gravesandestr", 'Thijs de Torenwachterstr', 'v Heiden Reinesteinln', 'V Hemert tot Dingshofpln', 'v Limburg Stirumstr', 'van Bloys van Treslongwg', 'van der Duyn v Maasdamln', 'van der Duyn v Maasdamwg', 'Van der Hardt Abersonln', 'van Stein Callenfelsstr', 'vd Duijn v Maasdamstr', 'vd Duyn v Maasdamstr', 'Verl Oude Veenendaalsewg', 'W Jehannes Koopmansstr', 'W Oude Aaltensewg', 'Wethouder Campermanstr', 'Willem van Rijswijckstr', 'Wouter van den Walestr', 'Zr Marie Adolphinestr', 'Zuster Dina Brondersstr']
blacklist_general = ['hoog','Hoog','HOOG','huis','laag','beneden','boven']
verdiepingen = ['1e','2e','3e','4e','5e','6e','7e','8e','9e', '1th','2th','3th','4th','5th','6th','7th','8th','9th']
blacklist_general2 = ['HAL','Hal', 'VLOER', 'Vloer','T.a.v.','Afd.','verd.','Dock', 'RECHTS','floor','FLOOR']
straatnaamdeel = ['akker','straat', 'laan','plein','weg','hof','gracht', 'dijk','steeg']
tm = ['t/m', 'T/m', 'T/M', 't/M']
postbus = ['postbus','po box','post bus','vo box','pastbus','past bus', 'p.o. box','p.o.box']
afkorting = {'strt':'straat','str.': 'straat','str':'straat','strat':'straat','wg.':'weg', 'wg' : 'weg','ln.': 'laan', 'ln' : 'laan','dk':'dijk','plin':'plein', 'straa': 'straat'}
straat_eind_cijfer = ['Plein', 'Rijksweg', 'Laan', 'Markerkant', 'Randstad', 'Weg', 'Verzetstraat 1940-', 'Provincialeweg', 'Provinciale weg', 'Plein 1945 -', 'Rijksweg nr', 'Laan 1940 -', 'Plein 1 febr', '', 'Singel', 'Brand', 'Autosnelweg', 'Antwerpseweg -', 'A', 'Zonegge', 'Gijsbertplein', 'Rijksweg A', 'Boulevard', 'Rozengaard', 'Archipel', 'Wold', 'Kamp', 'Zoom', 'Horst', 'Griend', 'Karveel', 'Kempenaar', 'De Veste', 'De Stelling', 'De Schans', 'Schouw', 'Botter', 'Tjalk', 'Grietenij', 'De Doelen', 'Kogge', 'Boeier', 'Punter', 'Oostvaardersdijk', 'Gondel', 'Jol', 'Galjoen', 'Schoener', 'Wijk', 'Langeloërduinen', 'Dalweg']
straat_eind_cijfer = [x.replace(' ','').lower() for x in straat_eind_cijfer]
straateind = ['str', 'weg', 'dijk', 'steeg', 'laan', 'akker', 'gracht', 'plein', 'hof','burg','park','sing','kade','vliet','plant','loan','lean','lang','kort','baan','vard','dat']


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

def drop_triplicate_letters(oldstring):
    newstring = oldstring[0]
    for char in oldstring[1:]:
        try:
            if char == newstring[-1] and char==newstring[-2] and char==newstring[-3] and char.isalpha():
                pass
            else:
                newstring += char
        except:
            newstring += char
            pass
    return newstring

def finding_another_hn(item):
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

#This function makes from a list a string.
def make_string(lijst):
    string=''
    for i in lijst:
        string+=' '+i
    return string.strip()

def replace_ins(repl, inpl, input):
    insensitive = re.compile(re.escape(repl), re.IGNORECASE)
    return insensitive.sub(inpl, input)

def number_removal(input):
    numbers = ['No.','NO.','no.']
    for n in numbers:
        input = input.replace(n,'')
    return input

def take_out_punct(item):
    if hasPunctuation(item):
        if ' - ' in item:
            item = item.replace(' - ','-')
        if ' -' in item:
            item = item.replace(' -','-')
        if '- ' in item:
            item = item.replace('- ','-')
        if ' / ' in item:
            item = item.replace(' / ','/')
        if ' /' in item:
            item = item.replace(' /','/')
        if '/ ' in item:
            item = item.replace('/ ','/')
        item = remove_double_spaces(item)
        output = []
        split = item.split()
        for n, i in enumerate(split):
            if hasNumbers(i) and hasPunctuation(i):
                for sp in string.punctuation:
                    if sp in i:
                        output.append(i.replace(sp,' '))
            else:
                output.append(i)
            if hasLetters(i)==False and n>0:
                return make_string(output).replace('  ', ' ')
        return make_string(output).replace('  ',' ')
    else:
        return item

def hasPunctuation(input):
    return any(char in string.punctuation for char in input)

#This function splits an element in a tuple of alphabet elements and digits.
def str_int_split(item):
    match = re.match(r"([a-z]+)([0-9]+)([a-z]+)", item, re.I)
    if match:
        items = match.groups()
        return items
    else:
        match = re.match(r"([a-z]+)([0-9]+)", item, re.I)
        if match:
            items = match.groups()
            return items
        else:
            match = re.match(r"([0-9]+)([a-z]+)([0-9]+)([a-z]+)", item, re.I)
            if match:
                items = match.groups()
                return items
            else:
                match = re.match(r"([0-9]+)([a-z]+)([0-9]+)", item, re.I)
                if match:
                    items = match.groups()
                    return items
                else:
                    match = re.match(r"([0-9]+)([a-z]+)", item, re.I)
                    if match:
                        items = match.groups()
                        return items

#remove comments or things between brackets with the brackets.
def remove_betweenbrackets(item):
    return re.sub(r'\([^)]*\)', '', item).strip()

#remove emails from strings.
def remove_email(input):
    input = input.split()
    for x in input:
        x = re.search(r"[^@]+@[^@]+\.[^@]+", x)
        if x != None:
            input.remove(x.group(0))
    return make_string(input)

#Clean function that removes most punctuations except: - and /'".
def clean(item):
    sign = '#$%&()*+,:;?@[\\]^_`{|}~'
    for i in sign:
        item = item.replace(i,' ').replace('  ',' ')
    for i in string.punctuation:
        item = item.strip(i).strip(' '+i).strip(i+' ')
    if '--' in item:
        item = item.replace('--','-')
    return item

#Take out weird ending words.
def take_out_endingwords(item):
    if 'verdieping' in item.lower():
        item2 = item.split()
        for i, el in enumerate(item2):
            if el.lower()=='verdieping':
                if len(item2[i-1])<4 and item2[i-1][0].isdigit():
                    item2.remove(item2[i-1])
                    item2.remove(el)
                elif len(item2[i+1])<4 and item2[i+1][0].isdigit():
                    item2.remove(item2[i+1])
                    item2.remove(el)
                item = make_string(item2)
    if hasNumbers(item) and item.split()[-1].isalpha() and len(item.split()[-1])>3 and hasNumbers(item.split()[0])==False:
        return item.split(item.split()[-1])[0].strip()
    else:
        return item

#Make street clean.
def clean_street(item):
    sign = '#$%&()*+,:;?@[\\]^_`{|}~'
    for i in sign:
        item = item.replace(i,' ').replace('  ',' ')
    for i in string.punctuation:
        item = item.strip(i).strip(' '+i).strip(i+' ')
    le = item.split()[-1]
    if hasNumbers(le):
        item = item.split(le)[0]
    return afkorting_to_full(item.strip())

def remove_double_spaces(item):
    while '  ' in item:
        item = item.replace('  ',' ')
    return item

def hasLetters(input):
    return any(char in string.ascii_letters for char in input)

#Checks if elements has numbers in it.
def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

#Make title from string.
def make_title(word):
    if word.strip()=="'T":
        return "'t"
    elif word[0].isdigit() or word[0]=="'" or word[0]=='"':
        return word
    else:
        return word.title()

#Find postcode in a string and removes it and gives back the string without postcode and the postcode seperate
def find_postcode(input):
    split = input.split()
    postcode = ''
    try:
        for i, el in enumerate(split):
            if len(el) == 6 and el[0:4].isdigit() and hasNumbers(el[4:5]) == False:
                postcode = el
                split.remove(el)
            elif len(el) == 4 and el[0:4].isdigit() and len(split[i+1])==2 and split[i+1].isalpha():
                postcode = el+split[i+1]
                split.remove(el)
                split.remove(split[i])
        if postcode == '':
            if len(split[-1])==2 and split[-1].isalpha() and len(split[-2])==4 and split[-2].isdigit():
                postcode = split[-2]+split[-1]
                split.remove(split[-2])
                split.remove(split[-1])
            elif len(split[1]) == 2 and split[1].isalpha() and len(split[0]) == 4 and split[0].isdigit():
                postcode = split[0] + split[1]
                split.remove(split[1])
                split.remove(split[0])
    except: pass
    return make_string(split), postcode

#Make name with the function make_title
def make_name(name):
    return make_string([make_title(i) for i in name.split()])

#Remove all the punctuations
def remove_shit(input):
    for p in '!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~':
        if p in input:
            input=input.replace(p,'')
    return input.strip()

#There are streets that end on abbreviations that we can transform back to street, weg, ... instead of str, wg, ...
def afkorting_to_full(street):
    if street not in blacklist_street:
        for element in list(afkorting.keys()):
            if element in street.lower():
                rex = re.compile(f'.*{element.lower()}$',flags=re.IGNORECASE)
                if rex.search(street) != None:
                    street = replace_ins(element+' ', afkorting[element],street+' ')
    return afkortingen_fix(street)

#Remove duplicate elements (adjacent elements)
def remove_adjacent(L):
    return [elem for i, elem in enumerate(L) if i == 0 or L[i-1] != elem or hasNumbers(str(elem))==False]

#Clean up extensions, where we take out words that are sometimes given but need to get out.
def clean_ext(input):
    for el in blacklist_general:
        if el in input:
            input = input.replace(el, '')
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

# This function splits the input and looks if there are hidden words connected with numbers, which are not extensions, such that we can split before main parsing part starts.
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
            if e.isdigit() and len(split[i+1])==1 and split[i+1].isalpha():
                output+=' '+e+split[i+1]
                del split[i+1]
            else:
                output+=' '+e
        except:
            output+=' '+e
            pass
    return output


def forgotten_space(input, straat_eind_cijfer):
    output = ''
    temp_input = ''
    flag=False
    for el in input.split():
        if '-' in el and hasNumbers(el):
            extra_el = el.split('-')[-1]
            if extra_el.isdigit() or extra_el.isalpha() and len(extra_el)<4:
                flag = True
        if is_all_uppercase(el) == False and is_all_lowercase(el) == False and hasPunctuation(el)==False and el.lower() not in straat_eind_cijfer:
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

def afkortingen_fix(input):
    if 'pres ' in input.lower():
        input = replace_ins('pres','President ',input)
    if 'kon.' in input.lower():
        input = replace_ins('kon.', 'Koning ', input)
    if 'pres.' in input.lower():
        input = replace_ins('pres.','President ',input)
    return input.replace('  ',' ')

#The parser function itself.
def parser(address):
    try:
        if address == None or address=='':
            return {'streetname': '', 'housenumber': '', 'double_hn': '', 'housenumber_ext': '', 'postcode': ''}

        huisnum = ''
        huisnum_ext = ''
        street = ''
        postcode = ''
        blacklist_used = False
        huisnum_2 = False

        #remove rubbish between brackets.
        if '(' and ')' in address:
            address = remove_betweenbrackets(address)

        #look for hidden postcode.
        address, postcode = find_postcode(clean(remove_email(address)))



        address = clean(address)

        if ',' in address:
            split = address.split(',')
            for s in split:
                for strt in straateind:
                    if strt.lower() in s.lower() and hasNumbers(s):
                        address = s
                    else:
                        pass

        if address.split()[-1].isdigit() == False:
            try:
                special_check = str_int_split(address.split()[-1])
                if len(special_check) == 3 and len(special_check[0]) == 1 and len(special_check[1]) == 2 and len(
                        special_check[2]) == 2 and special_check[0].isalpha() and special_check[1].isdigit() and \
                        special_check[2].isalpha():
                    address = clean(make_string(address.split()[0:-1]))
            except: pass

        #replace double spaces by one space
        address = forgotten_space(' '.join(address.split()), straat_eind_cijfer)


        #remove spaces around dashes.
        #this helps us later in the parsing process.
        if ' - ' in address:
            address = address.replace(' - ','-')
        if ' -' in address:
            address = address.replace(' -','-')
        if '- ' in address:
            address = address.replace('- ','-')
        if ' / ' in address:
            address = address.replace(' / ','/')
        if ' /' in address:
            address = address.replace(' /','/')
        if '/ ' in address:
            address = address.replace('/ ','/')

        #removing the t/m, T/M, etc...
        for t in tm:
            if t in address:
                address = address.split(t)[0]

        #Taking out symbols except dashes, points.
        address = drop_triplicate_letters(clean(take_out_endingwords(clean(address))))

        address = address.replace('-hg','').replace('-hoog','')

        #Checking if there are numbers in the addresses, which could be a housenumber or part of the streetname.
        if hasNumbers(address):

            #Take out double numbers (sometimes duplicates in data)
            #Split correct with above given function.
            split = remove_adjacent(splitting_correct(address))
            for i, element in enumerate(split):
                if element in blacklist_general2:
                    del split[i]
                    del split[i]


                elif len(element) > 4 and element.isdigit():
                    if int(element[0:4]) not in range(1940,1945):
                        del split[i]

                try:
                    if element in verdiepingen and split[i+1].lower() in ['et','etage','etag','et.','etag.','verdieping']:
                        del split[i]
                        del split[i]
                except:
                    pass

            #We found weird codes given in the addresses. We remove these, otherwise we can't find the addresses.

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

            #remove shit around elements.
            for i in '#$%&()*+,:;?@[\\]^_`{|}~':
                split = [x.strip(i).strip(' ' + i).strip(i + ' ') for x in split]

            # if split[-1].isdigit() and len(str(int(split[-1])))>4:
            #     address = address.replace(str(split[-1]),'')
            #     split = address.split()

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
            try:
                le2 = split[-2]
            except:
                le2 = ''
            if le.isdigit():
                if len(le)<8 and len(le)>4 and le.isdigit() and int(le[0:4]) in list(range(1940,1946)):
                    huisnum = le[4:]
                    street = make_string(split[0:-1])+' '+le[0:4]
                    return {'streetname': clean(make_name(street)), 'housenumber': huisnum, 'double_hn': huisnum_2,
                            'housenumber_ext': '', 'postcode': postcode.upper()}


                elif len(split) == 1 and len(str(split[0])) < 5:
                    huisnum = le
                    street = ''
                    huisnum_ext = ''

                # Check if second last element is digit this would give an extension number or maybe a streetname with a number in it. These special streets are given in blacklist_streetwithnum.
                elif '-' in le2 and hasNumbers(le2.split('-')[-1]):
                    if split[-3].isdigit()==False and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'').lower() in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le
                        street = make_string(split[0:-1])
                        huisnum_ext = ''
                    elif split[-3].isdigit() and make_string(split[0:-3]).replace(' ','').replace(le2.split('-')[-1],'').lower() in straat_eind_cijfer:
                        blacklist_used = True
                        huisnum = le2
                        street = make_string(split[0:-2])
                        huisnum_ext = le
                    elif split[-2].isdigit() and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'').lower() in straat_eind_cijfer:
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

                #CHEEEEEEEEEEEEEEECKK
                if len(split)==1:
                    return {'streetname': make_name(afkorting_to_full(split[0])), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext':'', 'postcode': postcode.upper()}

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

                if make_string(split[0:-1]+[lespl[0]]+['-']).replace(' ','') in straat_eind_cijfer:
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
                                return {'streetname': clean_street(make_name(address)), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext': '', 'postcode': postcode.upper()}
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
                            return {'streetname': clean_street(make_name(address)), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext': '',
                                    'postcode': postcode.upper()}

                elif make_string(split[0:-1]).replace(' ','').lower() in straat_eind_cijfer:
                    huisnum = le.split('-')[0]
                    huisnum_ext = le.split('-')[1]
                    street = make_string(split[0:-1])
                    if huisnum_ext.isdigit():
                        if int(huisnum)<int(huisnum_ext):
                            double_hn = True
                    return {'streetname': clean_street(make_name(street), afkorting),
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
                    return {'streetname': clean_street(make_name(street), afkorting),
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
                            huisnum_2 = True
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
                    return {'streetname': clean_street(make_name(address)), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext': '','postcode': postcode.upper()}

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
                            huisnum_2 = True
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
                    return {'streetname': clean_street(make_name(address)), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext': '',
                            'postcode': postcode.upper()}

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
                except:
                    pass

                #check if there are streets in the blacklist with last element number+extension. If so, we need to mark 'blacklist_used' as True.
                if '-' in le2 and le2.split('-')[-1].isdigit():
                    if split[-3].isdigit()==False and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'') in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-3].isdigit() and make_string(split[0:-3]).replace(' ','').replace(le2.split('-')[-1],'') in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-2].isdigit() and make_string(split[0:-2]).replace(' ','').replace(le2.split('-')[-1],'') in straat_eind_cijfer:
                        blacklist_used = True

                elif le2.isdigit():
                    if split[-3].isdigit()==False and make_string(split[0:-2]).replace(' ','') in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-3].isdigit() and make_string(split[0:-3]).replace(' ','') in straat_eind_cijfer:
                        blacklist_used = True
                    elif split[-2].isdigit() and make_string(split[0:-2]).replace(' ','') in straat_eind_cijfer:
                        blacklist_used = True

        # when there are no numbers in the address whatsoever, we give back the whole address as streetname..
        else:
            # huisnum = ''
            # huisnum_ext = ''
            street = address
            return {'streetname': clean_street(make_name(street)), 'housenumber': '', 'double_hn': huisnum_2,
                    'housenumber_ext':'', 'postcode': postcode.upper()}

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
                huisnum_2=True
        except:
            pass

        #Search for left-over numbers in the streetname
        hn_2, hne_2 = finding_another_hn(clean(street))
        if hn_2 != '' and len(hn_2)<4 and blacklist_used==False:
            huisnum = hn_2
            huisnum_ext = hne_2
            street = street.split(huisnum)[0]

        if street =='':
            return {'streetname': '', 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': huisnum_2,
                    'housenumber_ext': clean_ext(remove_shit(huisnum_ext)), 'postcode': postcode.upper()}
        else:
            if blacklist_used:
                return {'streetname': clean(make_name(street)), 'housenumber':remove_shit(str(int(huisnum))), 'double_hn': huisnum_2, 'housenumber_ext': clean_ext(remove_shit(huisnum_ext)), 'postcode': postcode.upper()}
            else:
                return {'streetname': clean_street(make_name(street)), 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': huisnum_2,
                        'housenumber_ext': clean_ext(remove_shit(huisnum_ext)), 'postcode': postcode.upper()}

    #we doe a try/except. If something goes wrong, we have a plan b for parsing.
    except:
        #plan b: search for a housenumber. Everything before is street, all the rest is extension.
        try:
            split = address.split()
            for i, element in enumerate(split):
                if element.isdigit():
                    huisnum = element
                    street = make_string(split[0:i])
                    if i!= (len(split)-1):
                        huisnum_ext = split[-1]
                    return {'streetname': clean_street(make_name(street)), 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': huisnum_2,
                            'housenumber_ext': clean_ext(huisnum_ext), 'postcode': postcode.upper()}
        except:
            try:
                return {'streetname': clean_street(make_name(street)), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext': '', 'postcode': postcode.upper()}
            except:
                return {'streetname': clean_street(make_name(address)), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext': '', 'postcode': postcode.upper()}


def parser_final(input):
    input = number_removal(input)
    postbusnum=''
    for pb in postbus:
        if pb.lower() in input.lower():
            m = re.search(f'{pb} (\d+)',input,re.IGNORECASE)
            if m is not None:
                postbusnum = m.group(1)
                input = re.sub(f'{pb} {postbusnum}','',input.lower(),re.IGNORECASE)
            else:
                m = re.search(f'{pb}(\d+)', input, re.IGNORECASE)
                if m is not None:
                    postbusnum = m.group(1)
                    input = re.sub(f'{pb}{postbusnum}', '', input.lower(), re.IGNORECASE)
                else:
                    input = re.sub(f'{pb}','',input.lower(),re.IGNORECASE)
    output = parser(input)

    if output is None:
        # print(f'Had 2 tries.')
        output = parser(take_out_punct(input))
    if output is None:
        # print(f'Hard to parse. Not sure if output is correct.')
        output={'streetname': input, 'housenumber': '', 'double_hn': False, 'housenumber_ext': '', 'postcode': '', 'extra': ''}
    output.update({'pobox': postbusnum})
    return output


# print(parser_final('Hout rijk 2151 DV'))