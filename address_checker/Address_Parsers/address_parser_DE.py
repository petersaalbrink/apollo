import re
import string

#streets that have str or ln end in the database
blacklist_street=["Bergfeldstr","Roggower Haffstr","Wächtersbachstr.",'Enge Str.','St.-Jürgen-Str.','St.-Jurgen-Str.','Bickestr.','Straße der Awg','Platz der Awg', 'Querstr. I', 'Querstr. II', 'Fliegerstr. 23', 'Fliegerstr. 23/Tower', 'Bundesstr. 87', 'Flugplatzstr. F 1', 'Flugplatzstr. F 2', 'Flugplatzstr. F 3', 'Ladestr. zum Güterbahnhof', 'Sandstr. Nord', 'Forststr. Waldhaus', 'Hauptstr. Philadelphia', 'Feldstr.Ausbau', 'Dorfstr. Krampfer', 'Dorfstr. Alt Krüssow', 'Dorfstr. Beveringen', 'Dorfstr. Giesensdorf', 'Dorfstr. Kemnitz', 'Dorfstr. Sadenbeck', 'Dorfstr. Sarnow', 'Dorfstr. Schönhagen', 'Dorfstr. Seefeld', 'Dorfstr. Steffenshagen', 'Dorfstr. Wilmersdorf', 'Hauptstr. Buchholz', 'Hauptstr. Falkenhagen', 'Hauptstr. Wilmersdorf', 'Dorfstr. Saalow', 'Schulstr. Saalow', 'Bundesstr. 96', 'Dorfstr. Ost', 'Dorfstr. West', 'Privatstr. 1', 'Privatstr. 10', 'Privatstr. 12', 'Privatstr. 13', 'Privatstr. 14', 'Privatstr. 2', 'Privatstr. 3', 'Privatstr. 4', 'Privatstr. 5', 'Privatstr. 6', 'Privatstr. 7', 'Privatstr. 8', 'Privatstr. 9', 'S-Bhf Yorckstr. (Großgörschenstr.)', 'Parkstr. Ost', 'Parkstr. West', 'Rheinstr. Ost', 'Rheinstr. West', 'Landesstr.A', 'Rennewartstr. Nord', 'Rennewartstr. Süd', 'Bundesstr. Schoren', 'Seestr. Ost', 'Seestr. West', 'Bundesstr. 11', 'Terminalstr. Mitte', 'Terminalstr. Ost', 'Terminalstr. West', 'Villenstr. Nord', 'Villenstr. Süd', 'Zubringerstr.-Autobahn', 'Gewerbestr.-Ost', 'Schopperstr. - Ost', 'Ringstr. Ost', 'Ringstr. West', 'An der Bundesstr. 19', 'Kreisstr. GZ 14', 'Bundesstr. 455', 'Hauptstr. Nord', 'Hauptstr. Süd', 'Ludwigstr. A.G.', 'Mühlstr. A.G.', 'Wächtersbachstr.A.G.', 'Ringstr. Nord', 'Ringstr. Süd', 'Umgehungsstr. Nord', 'Bundesstr. 44', 'Querstr. III', 'Querstr. IV', 'Südstr.I', 'Südstr.III', 'Hauptstr.-Ausbau', 'Feldstr.-Ausbau', 'Jahnstr.Ausbau', 'Verbindungsstr. Zweendamm', 'Lindenstr.-Ausbau', 'Dorfstr. Links', 'Dorfstr. Rechts', 'Bundesstr. 104', 'Wilhelmstr. Ausbau', 'An der Bundesstr. 188', 'Bundesstr. 4', 'Kreisstr. 22', 'Bundesstr. 1', 'Hauptstr. Mahlen', 'Deichstr. Ost', 'Deichstr. West', 'Morlaasstr. Ost', 'Morlaasstr. West', 'An der Bundesstr. 216', 'Heidhörnstr. Nord', 'Kanalstr. Nord', 'Kanalstr. Süd', 'Kanalstr. I', 'Kanalstr. II', 'Kanalstr.-Nord', 'Bundesstr. 70', 'Gewerbestr. links', 'Sandstr.-Nord', 'Sandstr.-Süd', 'Freitagstr.-Nord', 'Maxstr.-Privatweg', 'Freiheitstr. - neu', 'Gewerbestr. Süd', 'Kleine Kirchstr.- neu', 'Marienstiftstr. - neu', 'Rurstr. - neu', 'Schöffenstr. - neu', 'Unkelbachstr. - neu', 'Bundesstr. 235', 'Zementstr. A', 'Bauringstr. Nord', 'Bauringstr. Süd', 'An der Landstr. 820', 'Bickestr. I.', 'Bickestr. II.', 'Wasserstr./Bracht', 'Ladestr. Siegen-Ost', 'Industriestr. Möhnetal', 'Hauptstr. Katzenloch', 'Hauptstr. Willscheid', 'Weinstr. Nord', 'Weinstr. Süd', 'Bundesstr. 404', 'Bundesstr. Eesch', 'Schulstr.-Mitte', 'Schulstr.-West', 'Bundesstr. 5', 'An der Bundesstr. 207', 'Bundesstr. 202', 'Kreisstr. Wolfskrug', 'Bundesstr. 199', 'Bundesstr. 76', 'Bahnhofstr. Torsballig', 'Bundesstr. 205', 'Hauptstr. Steinburg', 'Alte Dorfstr. Barkhorst', 'Industriestr. Ost', 'Industriestr. West', 'Industriestr. A', 'Industriestr. C', 'Industriestr. B', 'Industriestr. D', 'Industriestr. E', 'Industriestr. H', 'Industriestr. K', 'Feldstr. Klietzen', 'Feldstr. Trinum', 'Kirchstr. Trinum', 'Bergstr.I', 'Bergstr.II', 'Bergstr. Abberode', 'Dorfstr. Biesenrode', 'Dorfstr. Braunschwende', 'Dorfstr. Gräfenstuhl', 'Dorfstr. Vatterode', 'Feldstr. Großörner', 'Hauptstr. Abberode', 'Hauptstr. Molmerswende', 'Klausstr. Saurasen', 'Mittelstr. Abberode', 'Mittelstr. Großörner', 'Schlossstr. Großörner', 'Schulstr. Abberode', 'Schulstr. Braunschwende', 'Schulstr. Großörner', 'Schulstr. Vatterode', 'Teichstr. Abberode', 'Unterstr. Abberode', 'Verbindungsstr. Großörner', 'Waldstr. Abberode', 'Dorfstr. Kauern', 'Bahnhofstr. Rampitz', 'Bauernstr. Schladenbach', 'Dorfstr. Kreypau', 'Dorfstr. Rampitz', 'Dorfstr. Schladebach', 'Dorfstr. Thalschütz', 'Dorfstr. Witzschersdorf', 'Dorfstr. Wölkau', 'Dorfstr. Wüsteneutzsch', 'Dorfstr. Trebnitz', 'Dorfstr. Lössen', 'Neue Dorfstr. II', 'Neue Dorfstr.I', 'Planstr. C', 'Dorfstr. nach Grub', 'Dorfstr. zum Feldstein', 'Dorfstr. zum Kalkofen', 'Bergstr.Nord', 'Burgstr.II']

straat_begin_cijfer = []
straat_eind_cijfer = ['Kreisstr.', 'Sommer', 'BAB A', 'An der L', 'Kreisstr. GZ', 'Lechstaustufe', 'Gewerbegebiet', 'Gewerbegebiet A', 'Straße des 28. April', 'An der B', 'M', 'Gewerbegebiet B', 'Demokratenbreite Weg', 'An der alten B', 'An de B', 'Jagen 17 -', 'An', 'An der L184', 'Flur 8-880', 'Raststätte Medenbach West BAB', 'Str. der B', 'Tankstelle Erbenheim Süd BAB', 'An der Alten F', 'Kolonie', 'Plan', 'P', 'Krummenort B', 'Straße der', 'Privatstr.', 'Gewerbepark A 61/B', 'KGA Oberspree', 'Coburger Forst', 'Bahnhaus Posten', 'KGA Sternwarte', 'Alte B', 'Rb-Flur 3-128', 'O', 'D', 'Kol. Friedland', 'Bundesstr.', 'Platz des 20. Juli', 'Im Jagen', 'An der E', 'H', 'Fußweg', 'B-96', 'KlGV', 'Kol. Lankwitz Stamm', 'Bungalowsiedlung Weißenspring', 'Am Galgenberg Dorf', 'Kugelfangberg', 'Feuerweg', 'An der B5', 'K', 'Dahmsdorfer Weg/Bung.S.', 'R', 'Zum Schuppen', 'A', 'Straße', 'Flugplatzstr. F', 'Bahnhaus an der B', 'Ausbau', 'Zollhaus', 'Interessentenweg', 'Raststätte Medenbach Ost BAB', 'An der Autobahn A', 'Straße Nr.', 'Verlängerte Straße', 'Tankstelle Erbenheim Nord BAB', 'Am Stich Maaß', 'An der Alten B', 'Gewerbegebiet Auf Herrel', 'An der K', 'An der BAB', 'Lechstufe', 'Rochow', 'Gewerbegebiet an der B','An der L113', 'KGA Neuer Garten', 'Blockstelle', 'Bahnhaus', 'Zum Schacht', 'An der A', 'Kol. An der Kappe', 'Landesstraße Nr.', 'Gewerbepark Neudahn', 'Auf den Kämpen Dorf', 'N', 'Kol. Südpark', 'Zeltplatz Tornow D', 'Flughafen P6', 'Weg', 'Am Hammelberg Weg', 'C', 'Businesspark A96', 'U', 'B-2', 'Waldhof an der B', 'Zeile', 'Jagen', 'F', 'Aussiedlerhof', 'An der Bundesstraße', 'Bahnhaus Nr.', 'Fliegerstr.', 'E', 'L', 'Autohof an der B', 'Außerhalb Königstädten L', 'An der B2', 'I', 'Außerhalb', 'Wochenendsiedlung Weißenspring', 'An de L', 'KGA Blankenburg Anlage', 'Aussiedlung B', 'Posten', 'An der Bundesstr.', 'Abfindungen Weg', 'G', 'T', 'Gewerbegebiet A61/L117', 'Auf der Hardt/An der B', 'Zeilhard Außerhalb', 'Ausbau Höhe', 'Feldweg', 'Bahnposten', 'Kol. Paulsfelde', 'Weg Nr.', 'Kol.Seebad,Block', 'Bungalowsiedlung K', 'Wochenendsiedlung Klixmühle', 'An der B 36/B', 'Ziegelei Werk Abt.', 'Autobahn A61', 'Hartward an der K', 'Bungalowsiedlung', 'Campingplatz E', 'Siedlung', 'An der Landstr.', 'Straße an der F', 'B', 'S', 'Rasthaus B', 'Atlantis Haus', 'Bahnwärterhaus', 'Schacht', 'Ostbense an der L', 'Hof', 'Siedlung an der F', 'Haus', 'Landesstraße', 'Q', 'Gewerbepark BAB', 'An der Halle']
straat_eind_cijfer = [x.replace(' ','').lower() for x in straat_eind_cijfer]

blacklist_general = ['dhl','links','hinterhaus','hausnummer']
# verdiepingen = ['1e','2e','3e','4e','5e','6e','7e','8e','9e', '1th','2th','3th','4th','5th','6th','7th','8th','9th']
blacklist_general2 = ['hal ', 'vloer ', 'floor ']
# straatnaamdeel = ['akker','straat', 'laan','plein','weg','hof','gracht', 'dijk','steeg']
tm = ['t/m', 'T/m', 'T/M', 't/M']
lowercase_words =[]
#note that the order of this list is made in such a way to work optimal.
postbus = ['DHL postfach','Postfach','P.O. box','PO box','DHL-Postnummer','Postnummer','p.o.box']
packstation = ['DHL Packstation', 'DHL-Packstation','Packstation','Grossempfänger','Großempfänger','DHL Postfiliale','Postfiliale']
boxes = ['bus','box','tor']

afkorting = {'str.':'straße','stra':'straße','str':'straße','wg.':'weg','wg':'weg', 'strae':'straße','stra e':'straße','stra?e':'straße','strase':'straße'}
straateind = ['str','weg','biet','breit','bach','wg','land','park','halle','burg','vard']


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


#This function makes from a list a string.
def make_string(lijst):
    string=''
    for i in lijst:
        string+=' '+i
    return string.strip()


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

def replace_ins(repl, inpl, input):
    insensitive = re.compile(re.escape(repl), re.IGNORECASE)
    return insensitive.sub(inpl, input)

def number_removal(input):
    numbers = ['No.','NO.','no.']
    for n in numbers:
        input = input.replace(n,'')
    return input

def take_digits(input):
    return re.search(r"(\d+)*", input).group()

def take_out_punct(item):
    if haspunctuation(item):
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
            if hasNumbers(i) and haspunctuation(i):
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
    try:
        le = item.split()[-1]
        if hasNumbers(le):
            item = item.split(le)[0]
        for ps in packstation:
            if ps in item and ps.lower().strip()!=item.lower().strip():
                item=replace_ins(ps,'',item)
    except: pass
    return clean(afkorting_to_full(item.strip()))

def remove_double_spaces(item):
    while '  ' in item:
        item = item.replace('  ',' ')
    return item

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
    if '(' and ')' in input:
        split = input.split()
        for el in split:
            if '(' and ')' in el and hasNumbers(el):
                post = el.replace(')','').replace('(','')
                if post.isdigit() and len(post)==5:
                    return input, post
    input = input.split()
    postcode = ''
    for i, el in enumerate(input):
        if len(el) == 5 and el[0:5].isdigit():
            postcode = el
            input.remove(el)
            return make_string(input), postcode
        else:
            for p in '.,/-':
                if p in el:
                    split = el.split(p)
                    for s in split:
                        if len(s) == 5 and s[0:5].isdigit():
                            postcode = s
                            input = make_string(input)
                            return input, postcode
    return make_string(input), postcode


    # except:
    #     return make_string(input), ''
    #     pass

#Make name with the function make_title
def make_name(name):
    output = make_string([make_title(i) for i in name.split()])
    for lw in lowercase_words:
        output = output.replace(f'{lw} ',f'{lw.lower()} ')
    return output

#Remove all the punctuations
def remove_shit(input):
    for p in string.punctuation:
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
    return street

#Remove duplicate elements (adjacent elements)
def remove_adjacent(L):
    return [elem for i, elem in enumerate(L) if i == 0 or L[i-1] != elem or hasNumbers(str(elem))==False]

#Clean up extensions, where we take out words that are sometimes given but need to get out.
def clean_extra_words(input):
    for el in blacklist_general:
        if el.lower() in input.lower():
            input = replace_ins(el,'',input)
    return input


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


def hasPunctuation(input):
    return any(char in string.punctuation for char in input)

def hasLetters(input):
    return any(char in string.ascii_letters for char in input)


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

def haspunctuation(input):
    return any(char in string.punctuation for char in input)

#The parser function itself.
def parser(address):
    try:
        if address == None or address=='':
            return {'streetname': '', 'housenumber': '', 'double_hn': False, 'housenumber_ext': '', 'postcode': '', 'extra': ''}

        hn_box=False
        hne_box =False
        street_extra =False
        huisnum = ''
        huisnum_ext = ''
        extra_info = ''
        street = ''
        postcode = ''
        blacklist_used = False
        huisnum_2 = False

        #remove rubbish between brackets.
        if '(' and ')' in address:
            address = remove_betweenbrackets(address)

        #look for hidden postcode.
        address, postcode = find_postcode((remove_email(address)))

        #replace double spaces by one space
        address = forgotten_space(' '.join(address.split()).replace('?','ß'), straat_eind_cijfer).replace(':','').replace('strabe','straße').replace('Strabe','Straße')

        if ',' in address:
            split = address.split(',')
            for s in split:
                for strt in ['straße', 'strasse', 'weg','steg','unteres','ufer','wiese','hof','kamp','große','grosse','gasse', 'graben','damm','chaussee','bahnhof','bach','bad', 'autobahn']:
                    if strt.lower() in s.lower() and hasNumbers(s):
                        address = s
                    else:
                        pass


        #clean more
        address = clean(address)

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
        address = drop_triplicate_letters(replace_ins('strasse ','straße ',clean(take_out_endingwords(clean_extra_words(clean(address))))))

        #Checking if there are numbers in the addresses, which could be a housenumber or part of the streetname.
        if hasNumbers(address):

            #Take out double numbers (sometimes duplicates in data)
            #Split correct with above given function.
            split = remove_adjacent(splitting_correct(address))

            #Checking if there are elements such as éme, de, De, or other parts of extensions such as box, bus, etc. that we could take out or use as extra information.
            for i, element in enumerate(split):
                if len(element)==3 and element[0].isdigit() and element[1]+element[2]=='th':
                    del split[i]

                elif len(element)>4 and element.isdigit():
                    del split[i]

                if element.lower() in blacklist_general2:
                    del split[i]
                    del split[i]

                if len(element)==6 and element[0:4].lower()=='stra' and element[-1]=='e':
                    split[i]= 'Straße'

                for b in boxes:
                    if element.lower() == b:
                        extra_info = make_string(split[i:]).title()
                        del split[i]
                        for n in range(len(split[i:])):
                            del split[i+n]
                        hne_box = True
                    if hne_box==False and b in element.lower() and hasNumbers(element.lower()):
                        if '/' in element:
                            element = element.replace('/','')
                        if '-' in element:
                            element = element.replace('-','')
                        strint = str_int_split(element)
                        try:
                            if strint is None:
                                element = element+split[i+1]
                                if split[i-1].isdigit() and hasNumbers(split[0:i-2])==False:
                                    element = split[i-1]+element
                                strint = str_int_split(element)
                        except:
                            pass
                        if strint is not None:
                            for j, e in enumerate(strint):
                                if b in e.lower():
                                    try:
                                        if len(split)>len(split[0:i]) and split[i+1].isdigit():
                                            extra_info = b.title()+' '+make_string(strint[j+1:]).replace(' ','')+split[i+1]
                                            huisnum_ext = replace_ins(b, '', e)
                                            hne_box = True
                                            street = make_string(split[0:i])
                                            street_extra=True
                                            if strint[0].isdigit():
                                                huisnum_box = strint[0]
                                                hn_box = True
                                        else:
                                            extra_info =b.title()+' '+make_string(strint[j+1:]).replace(' ','')
                                            huisnum_ext = replace_ins(b, '', e)
                                            hne_box = True
                                            street = make_string(split[0:i])
                                            street_extra = True
                                            if strint[0].isdigit():
                                                huisnum_box = strint[0]
                                                hn_box = True
                                    except:
                                        try:
                                            if hasNumbers(split[i+1]):
                                                extra_info =b.title() + ' ' + split[i+1]
                                                huisnum_ext = replace_ins(b, '', e)
                                                hne_box = True
                                                street = make_string(split[0:i])
                                                street_extra = True
                                                if strint[0].isdigit():
                                                    huisnum_box = strint[0]
                                                    hn_box = True
                                        except:
                                            pass
            address= make_string(split)
            if hne_box:
                if address == replace_ins(huisnum_ext+extra_info, '', address):
                    address = clean(replace_ins(huisnum_ext+extra_info.replace(' ',''), '', address))
                else:
                    address = clean(replace_ins(huisnum_ext+extra_info, '', address))

                if hn_box:
                    address = replace_ins(huisnum_box, '', address)
                    if street_extra:
                        street = clean(street)
                    else:
                        street = clean(address)
                    if len(huisnum_box)<3:
                        return {'streetname': clean_street(make_name(street)), 'housenumber': huisnum_box, 'double_hn': huisnum_2,
                                'housenumber_ext': clean_ext(huisnum_ext),
                                'postcode': postcode.upper(), 'extra': extra_info}
                    if hasNumbers(split[-1]) and '-' in split[-1]:
                        split_hn = split[-1].split('-')
                        if take_digits(split_hn[0]).isdigit():
                            huisnum_box = split_hn[0]
                            if take_digits(split_hn[1]).isdigit() and int(take_digits(split_hn[0]))>int(take_digits(split_hn[1])):
                                huisnum_ext = take_digits(split_hn[1])
                            elif take_digits(split_hn[1]).isdigit():
                                huisnum_ext = take_digits(split_hn[1])
                            elif split_hn[1].isalpha():
                                huisnum_ext = split_hn[1]
                        street = replace_ins(split[-1], '', street)
                        return {'streetname': clean_street(make_name(street)), 'housenumber': huisnum_box, 'double_hn': huisnum_2,
                                'housenumber_ext': clean_ext(huisnum_ext),
                                'postcode': postcode.upper(), 'extra': extra_info}
                    if hasNumbers(split[-2]) and '-' in split[-2]:
                        split_hn = split[-2].split('-')
                        if take_digits(split_hn[0]).isdigit():
                            huisnum_box = split_hn[0]
                            if take_digits(split_hn[1]).isdigit() and int(take_digits(split_hn[0]))>int(take_digits(split_hn[1])):
                                huisnum_ext = take_digits(split_hn[1])
                            elif take_digits(split_hn[1]).isdigit():
                                huisnum_ext = take_digits(split_hn[1])
                            elif split_hn[1].isalpha():
                                huisnum_ext = split_hn[1]
                            street = replace_ins(split[-2],'',street)
                        try:
                            if int(huisnum_box)<int(clean_ext(huisnum_ext)):
                                huisnum_2 = True
                        except:
                            pass
                        return {'streetname': clean_street(make_name(street)), 'housenumber': huisnum_box, 'double_hn': huisnum_2,
                                'housenumber_ext': clean_ext(huisnum_ext),
                                'postcode': postcode.upper(), 'extra': extra_info}
                    elif hasNumbers(split[-1]) and '/' in split[-1]:
                        split_hn = split[-1].split('/')
                        if take_digits(split_hn[0]).isdigit():
                            huisnum_box = split_hn[0]
                            if take_digits(split_hn[1]).isdigit() and int(take_digits(split_hn[0]))>int(take_digits(split_hn[1])):
                                huisnum_ext = take_digits(split_hn[1])
                            elif take_digits(split_hn[1]).isdigit():
                                huisnum_ext = take_digits(split_hn[1])
                            elif split_hn[1].isalpha():
                                huisnum_ext = split_hn[1]
                        street = replace_ins(split[-1], '', street)
                        try:
                            if int(huisnum_box)<int(clean_ext(huisnum_ext)):
                                huisnum_2 = True
                        except:
                            pass
                        return {'streetname': clean_street(make_name(street)), 'housenumber': huisnum_box, 'double_hn': huisnum_2,
                                'housenumber_ext': clean_ext(huisnum_ext),
                                'postcode': postcode.upper(), 'extra': extra_info}
                    elif hasNumbers(split[-2]) and '/' in split[-2]:
                        split_hn = split[-2].split('/')
                        if take_digits(split_hn[0]).isdigit():
                            huisnum_box = split_hn[0]
                            if take_digits(split_hn[0]).isdigit() and int(take_digits(split_hn[0]))>int(take_digits(split_hn[1])):
                                huisnum_ext = take_digits(split_hn[1])
                            elif take_digits(split_hn[1]).isdigit():
                                huisnum_ext = take_digits(split_hn[1])
                            elif split_hn[1].isalpha():
                                huisnum_ext = split_hn[1]
                        street = replace_ins(split[-2], '', street)
                        try:
                            if int(huisnum_box)<int(clean_ext(huisnum_ext)):
                                huisnum_2 = True
                        except:
                            pass
                        return {'streetname': clean_street(make_name(street)), 'housenumber': huisnum_box, 'double_hn': huisnum_2,
                                'housenumber_ext': clean_ext(huisnum_ext),
                                'postcode': postcode.upper(), 'extra': extra_info}
                    else:
                        try:
                            if int(huisnum_box)<int(clean_ext(huisnum_ext)):
                                huisnum_2 = True
                        except:
                            pass
                        return {'streetname': clean_street(make_name(street)), 'housenumber': huisnum_box, 'double_hn': huisnum_2, 'housenumber_ext': clean_ext(huisnum_ext),
                                        'postcode': postcode.upper(), 'extra': extra_info}



            #remove shit around elements.
            for i in '#$%&()*+,:;?@[\\]^_`{|}~':
                split = [x.strip(i).strip(' ' + i).strip(i + ' ') for x in split]

            if split[-1].isdigit() and len(str(int(split[-1])))>4:
                address = address.replace(str(split[-1]),'')
                split = address.split()

            #housenumber and address name are swapped we should swap back before performing parsing.
            if hasNumbers(split[-1])== False and split[0].isdigit():
                address = make_string(split[1:]+[split[0]])
                split = address.split()
            elif hasNumbers(split[0]) and split[0][0].isdigit() and split[-1][0].isdigit()== False:
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
                if len(split) == 1 and len(str(split[0])) < 5:
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
                    if huisnum == '' and postcode != '':
                        huisnum = postcode
                        postcode = ''
                    return {'streetname': make_name(afkorting_to_full(split[0])), 'housenumber': huisnum, 'double_hn': huisnum_2, 'housenumber_ext':clean_ext(huisnum_ext), 'postcode': postcode.upper(), 'extra': extra_info}

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
                                    'postcode': postcode.upper(), 'extra': extra_info}

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
                        # if int(lespl[0])<int(lespl[1]) and int(lespl[1])-int(lespl[0])<=4:
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
                        elif int(lespl[0]) == int(lespl[1]) and int(lespl[0])<5:
                            huisnum = lespl[0]
                            huisnum_ext = lespl[1]
                            street = make_string(split[0:-1])
                        elif int(lespl[0]) == int(lespl[1]) and int(lespl[0])>=5:
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
                    return {'streetname': clean_street(make_name(address)), 'housenumber': '', 'double_hn': huisnum_2, 'housenumber_ext': '','postcode': postcode.upper(), 'extra': extra_info}

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
                            huisnum_2 = True
                            street = make_string(split[0:-1])
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
                    return {'streetname': clean_street(make_name(address)), 'housenumber': huisnum, 'double_hn': huisnum_2, 'housenumber_ext': '',
                            'postcode': postcode.upper(), 'extra': extra_info}

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

                #check if there are streets in the blacklist with last element number+extension. If so, we need to flag 'blacklist_used' as True.
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
            street = address
            return {'streetname': clean_street(make_name(street)), 'housenumber': huisnum, 'double_hn': huisnum_2,
                    'housenumber_ext':'', 'postcode': postcode.upper(), 'extra': extra_info}
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

        #
        if hn_box:
            huisnum = huisnum_box

        # if huisnum == '' and postcode != '' and street!='':
        #     huisnum = postcode
        #     postcode = ''

        # if huisnum_ext.isdigit() and int(huisnum_ext)>int(huisnum) and int(huisnum_ext)>9:
        #     huisnum_ext = ''

        if huisnum.isdigit() and len(huisnum)>5:
            huisnum = ''

        try:
            if int(huisnum)<int(huisnum_ext):
                huisnum_2=True
        except:
            pass

        if street =='':
            return {'streetname': '', 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': huisnum_2,
                    'housenumber_ext': clean_ext(huisnum_ext), 'postcode': postcode.upper(), 'extra': extra_info}
        else:
            hn_2, hne_2 = finding_another_hn(clean(street))
            if hn_2 != '' and len(hn_2) < 4 and blacklist_used==False:
                huisnum = hn_2
                huisnum_ext = hne_2
                street = street.split(huisnum)[0]

            if blacklist_used:
                return {'streetname': clean(make_name(street)), 'housenumber':remove_shit(str(int(huisnum))), 'double_hn': huisnum_2, 'housenumber_ext': clean_ext(huisnum_ext), 'postcode': postcode.upper(), 'extra': extra_info}
            else:
                return {'streetname': clean_street(make_name(street)), 'housenumber': remove_shit(str(int(huisnum))), 'double_hn': huisnum_2,
                        'housenumber_ext': clean_ext(huisnum_ext), 'postcode': postcode.upper(), 'extra': extra_info}

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
                            'housenumber_ext': clean_ext(huisnum_ext), 'postcode': postcode.upper(), 'extra': extra_info}
        except:
            try:
                return {'streetname': clean_street(make_name(street)), 'housenumber': '', 'double_hn': False, 'housenumber_ext': '', 'postcode': postcode.upper(), 'extra': ''}
            except:
                return {'streetname': clean_street(make_name(address)), 'housenumber': '', 'double_hn': False, 'housenumber_ext': '', 'postcode': postcode.upper(), 'extra': ''}


def parser_final(input):
    input = number_removal(input).replace(':','')
    postbusnum=''
    for pb in postbus:
        if pb.lower() in input.lower():
            m = re.search(f'{pb.lower()} (\d+)',input,re.IGNORECASE)
            if m is not None:
                postbusnum = m.group(1)
                input = re.sub(f'{pb.lower()} {postbusnum}','',input.lower(),re.IGNORECASE)
                input = re.sub(f'{pb.lower()}', '', input.lower(), re.IGNORECASE)
            else:
                m = re.search(f'{pb.lower()}(\d+)', input, re.IGNORECASE)
                if m is not None:
                    postbusnum = m.group(1)
                    input = re.sub(f'{pb.lower()}{postbusnum}', '', input.lower(), re.IGNORECASE)
                else:
                    input = re.sub(f'{pb.lower()}','',input.lower(),re.IGNORECASE)
            if postbusnum.isdigit():
                postbusnum = pb+' '+postbusnum
                input = re.sub(f'{postbusnum}', '', input.lower(), re.IGNORECASE)
    output = parser(input)
    if output is None:
        output = parser(take_out_punct(input))
    if output is None:
        try:
            street = clean(clean_street(make_name(input)))
            hn_2, hne_2 = finding_another_hn(input)
            street = clean(clean_street(make_name(input))).split(hn_2 + hne_2)[0]
            output={'streetname': street, 'housenumber': hn_2, 'double_hn': False, 'housenumber_ext': clean_ext(hne_2), 'postcode': '', 'extra': '','pobox': ''}
        except:
            try:
                hn_2, hne_2 = finding_another_hn(input)
                street = clean(clean_street(make_name(input))).split(hn_2+hne_2)[0]
                output = {'streetname': street, 'housenumber': hn_2, 'double_hn': False, 'housenumber_ext': clean_ext(hne_2), 'postcode': '', 'extra': '','pobox': ''}
            except:
                output = {'streetname': input, 'housenumber': '', 'double_hn': False, 'housenumber_ext': '',
                          'postcode': '', 'extra': '', 'pobox': ''}
    output.update({'pobox': postbusnum})
    return output