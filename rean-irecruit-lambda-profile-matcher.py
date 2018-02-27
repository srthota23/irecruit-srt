from __future__ import print_function

import os
import json
import urllib
import boto3

print('...Starting AWS Lambda function irecruit-keyword-search.py...')

s3 = boto3.resource('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):

    # Read the resume from the event and split it into a list
    resume_bucketname = event['Records'][0]['s3']['bucket']['name'] # Read bucket name
    resume_filename = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8')) # Read resume name
    obj = s3.Object(resume_bucketname, resume_filename)
    resume = obj.get()['Body'].read().decode('utf-8') # Read resume's body
    resume = resume.replace('\n', '').lower()
    nomatchprofile = [] # List of profiles that don't match any skills
    scoreList = [] # List of initial score order
    fileNameList = [] # List of initial profile order
    fileNameListReorder = [] # List for profile names descenting from highest score
    fullDict = {} # Dictionary to hold all neccessary information for each profile

    result = '''
* * * * * * * * * * * *

Resume: '''
    result = result + resume_filename + "\n"
    result+= "Below are the matches for each skillset: "
    
    # read all the job profiles and match it against the candidate resume
    profile_bucketname = os.environ['Profiles_bucket']
    profile_bucket = s3.Bucket(profile_bucketname)
    for profile in profile_bucket.objects.all(): # For loop of all profiles to compile skills, matches, scores
        profile_filename = profile.key.encode('utf-8')
        #profile_obj = profile.Object()
        profile_obj = s3.Object(profile_bucketname, profile_filename)
        profile_body = profile_obj.get()['Body'].read().decode('utf-8')
        skills = profile_body.lower().split('\n') #Skills with weight separated ny comma
        skillsList = [(x.partition(',')[0]).lower() for x in skills] #Just skills
        weightDict = {} #empty dictionary for weight lookup
        
        for x in range(0,len(skills)): # For loop to create dictionary of weights for each skill
            a = skills[x]
            b = a.partition(',') #tuple creation to separate weight and skill
            weightDict[b[0].strip().lower()] = b[2].strip() #creating dictionary with skill and weight 
           
        match = 0 #intializing for the number of each profiles's skill match
        matchlist = [] #intializing for the list each profiles's skill match
        nomatchlist = [] #intializing for the list each profiles's skill not matching
     
        for skill in skillsList: # For loop to add up score and match and no-match lists
            if skill in resume:
                matchlist.append(skill)
                match = match + 1
            else:
                nomatchlist.append(skill)
        
        score = 0.0 # Intializing score
        maxScore = 0.0 # Intializing max score
        
        for skill in skillsList: # For loop to calculate max score if all skills matched
            weight = float(weightDict[skill])
            maxScore = maxScore + weight/10.0
        
        for skill in matchlist:  # For loop to calculate score for skills matched
            weight = float(weightDict[skill])
            score = score + weight/10.0

        adjScore = round(float(score/maxScore*100),1) # Formula to account for weighted score
        
        if adjScore == 0.0: #if statement to add profile to no skill profile list
            nomatchprofile.append(profile_filename)
            continue # skip next statement if profile has score
        
        if profile_bucketname not in nomatchprofile: # If statement to get if profile has a score and 
                                                     # add score, matching skills, no matching skills, # of skill, and # of no match skills to a dictionary of each profiles' dictionary
            scoreList.append(adjScore) # intial order adding of skill match scores
            fileNameList.append(profile_filename)  # intial order adding of profile names 
            fullDict[profile_filename] = {'score': adjScore,'matchingSkills': str(len(matchlist)),'noSkills': str(len(nomatchlist)), 'matchlist':matchlist,'nomatchlist':nomatchlist }
           
    length = len(scoreList) # number of scores
    scoreListSorted = sorted(scoreList) # re order score list from smalled to largest
    indexPro = list('x')*length # list to get index each intial order profiles' ranking
    
    for x in range(0,length): # for loop to reorder for largest to smallest, mirrored list
        y = length-1-x
        scoreListSorted.append(float(scoreListSorted[y]))
        
    scoreListSorted = scoreListSorted[length:] # concatenate beginning ascending part of list
    
    for y in range(0,length): # for loop to get index of skills' ranking 
        ind =  scoreListSorted.index(scoreList[y])
        if ind not in indexPro:
            indexPro[y] = ind
        else: 
            ind = ind + 1
            indexPro[y] = ind
        
    for z in range(0,length): # for loop to find profile ranking and reorder profile list
        position = indexPro.index(z)
        fileNameListReorder.append(fileNameList[position])
        
    for profiles in fileNameListReorder: # print out loop for each in profile in descending profile score
        adjScore = fullDict[profiles]['score']
        matchingSkills = int(fullDict[profiles]['matchingSkills'])
        noSkills = int(fullDict[profiles]['noSkills'])
        nomatchlist = fullDict[profiles]['nomatchlist']
        matchlist = fullDict[profiles]['matchlist']
        result += "\n\n++++++\n"
        result = result + "Profile Name: " + profiles + "\n"
        result += "Profile Score: "+str(adjScore)+"%\n\n"
        metrics = "This resume matches " + str(matchingSkills) + " of " + str(matchingSkills + noSkills) + " skills from this profile\n"
        result += metrics
        result += "\nSkills that match: \n"
        result += ", ".join(matchlist)
        result += "\n\nSkills that don't match: \n"
        result += ", ".join(nomatchlist)
        result += "\n++++++\n"
    
    if len(nomatchprofile) > 0: # if and for loop to add a print out only if one or more profiles do not match
        result += "\n++++++\n"
        result += "\nProfile(s) without skill match:\n\n"
        for y in range(0,len(nomatchprofile)):
            result += nomatchprofile[y] + "\n"
    
    result += "\n* * * * * * * * * * * *\n"
    
    response = sns.publish(
        TopicArn=os.environ['SNS_Topic_ARN'],
        Message=result,
        Subject='Skills match for ' + resume_filename)
    return indexPro, scoreList, scoreListSorted
    