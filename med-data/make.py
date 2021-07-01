from random import randint, choice
import datetime
import sys

def generate(i):
    patient_line = generate_patient(i)
    tag = 0
    times = generate_date()
    for time in times:
        tag_1 = 0
        operating_tag = 0
        event_number = randint(10, 30)
        for j in range(event_number):
            event, event_line = generate_event(tag)
            if (tag == 1) & (event == 'operating'):
                continue
            if (event == 'observation end') & (tag == 0):
                continue
            if (event == 'observation start') & (tag == 1):
                continue
            if (event == 'effective') & (operating_tag == 1):
                continue
            if (event == 'operating') & (tag_1 == 1):
                continue
            write(patient_line, time, event_line)
            if event == 'observation start':
                tag = 1
                tag_1 = 1
            if event == 'observation end':
                tag = 0
            if event == 'operating':
                operating_tag = 1
    if tag == 1:
        event, event_line = generate_event(tag, 'observation end')
        write(patient_line, time, event_line)
    end_event = choice(end_events)
    event, event_line = generate_event(tag, end_event)
    write(patient_line, time, event_line)

def generate_date():
    day = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d')
    times = []
    for i in range(randint(10, 180)):
        times.append(day.strftime('%Y-%m-%d'))
        day = day + datetime.timedelta(days=1)
    return times

def generate_patient(i):
    patient = str(i)
    institution = institutions[randint(0, 4)]
    location = generate_location()
    casetype= casetypes[randint(0, 1)]
    unit_num = randint(0, 1)
    unit = units[unit_num]
    parts = unit.split(',')
    bed = int(parts[2]) + 1
    if bed > 20:
        bed = bed % 20
        parts[1] = parts[1][0] + str(int(parts[1][1]) + 1)
    parts[2] = str(bed)
    unit = ''
    for part in parts:
        unit += part + ','
    unit = unit[0:-1]
    units[unit_num] = unit
    line = location + ',' + patient + ',' + institution + ',' + unit + ',' + casetype + ',' + o_comment + ',' + f_comment + ',' + b_comment
    return line

def generate_location():
    regions = ['A', 'B', 'C', 'D']
    region = choice(regions)
    block = region + str(randint(1, 5))
    return 'SG,' + region + ',' + block

def generate_event(tag, event=None):
    if event == None:
        while 1:
            event = events[randint(0, 14)]
            if tag == 1:
                if event in ['observation start', 'death', 'discharge']:
                    continue
            break
    metric = str(randint(0, 100))
    doctor = str(randint(0, 47))
    line = event + ',' + metric + ',' + doctor + ',' + 'aaa'
    return event, line

def write(patient_line, date, event_line):
    with open('data.csv', 'a+') as f:
        f.write(patient_line + ',' + date + ',' + event_line + '\n')

if __name__ == '__main__':

    institutions = ['AH', 'NUS', 'TTSH', 'IMH', 'NHGP']
    units = ['ED,E1,0', 'ICU,I1,0']
    casetypes = ['INPATIENT', 'OUTPATIENT']
#    patient_number = 10000
    events = ['pulse rate', 'blood pressure', 'respiratory rate', 'SAO2', \
		'temperature', 'pain score', 'medication', 'total(GCS)', 'effective', \
		'performed', 'observation start', 'observation end', 'radiology', 'movement', 'operating']
    end_events = ['discharge', 'death']
    o_comment = 'Full description of the code that indicates Assigned Organization Unit Identification Number.'
    f_comment = 'Assigned patient facility - a more precise description of the facility where the patient is located. Can include a building. part of a building. a mobile clinic that provides health care services.'
    b_comment = 'Indication of a bed assigned to a  patient.'
    
    beginUser = int(sys.argv[1])
    endUser = int(sys.argv[2])
    for i in range(beginUser, endUser):
        generate(i)
