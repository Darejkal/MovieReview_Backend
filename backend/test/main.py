import time
import requests
import os
import json
from typing import Dict, List
import re
class GradeParser:
    def __init__(self) -> None:
        pass
    grade_exception=set()
    grade_map = {
        "F-": 0,
        "F": 1,
        "F+": 2,
        "D-": 3,
        "D": 4,
        "D+": 5,
        "C-": 6,
        "C": 7,
        "C+": 8,
        "B-": 9,
        "B": 10,
        "B+": 11,
        "A-": 12,
        "A": 13,
        "A+": 14
    }
    def parseGrade(self,grade:str):
        if grade is None:
            return None
        grade=grade.replace(" ","")
        try:
            grade=float(grade)
            max_grade=1
            while(max_grade<grade):
                max_grade*=10
            return grade/max_grade
        except:
            pass
        num_score=re.search(r"(?P<first>(\d+\.*\d*)|(\.\d+))/(?P<second>(\d+\.*\d*)|(\.\d+))",grade)
        if num_score and num_score.group("first") and num_score.group("second"):
            try:
                return float(num_score.group("first"))/float(num_score.group("second"))
            except:
                return grade
        alphabet_score=re.search(r"(\w) *(\+|-)*",grade)
        if alphabet_score and alphabet_score.group(1):
            try:
                submark=alphabet_score.group(2)
            except:
                pass
            if not submark:
                submark=""
            _abcgrade=alphabet_score.group(1).upper()+submark
            if _abcgrade in self.grade_map:
                return self.grade_map[_abcgrade]/self.grade_map["A+"]
        self.grade_exception.add(grade)
        # print(f"\{grade}\\")
        return grade
    def genMovieProcessed(self,path:str="review_list"):
        with open(path,"r") as f:
            for line in f:
                movie:Dict=json.loads(line)
                for k,v in movie.items():
                    movie=re.search(r"https://www\.rottentomatoes\.com/m/([^/]+)",k)
                    if movie is not None:
                        movie=movie[1]
                    for review in v:
                        additional:List[str]=[r.strip() for r in review["additional"].split("|")]
                        grade,date=None,None
                        if len(additional)==3:
                            _grade=re.search(r"Original Score: +(.+)",additional[1]).group(1)
                            grade=self.parseGrade(_grade)
                            if(grade and type(grade)!= str and grade>1 ):
                                # print("Discard parsed",_grade)
                                grade=_grade
                            date=additional[2]
                        elif len(additional)==2:
                            date=additional[1]
                        else:
                            raise NotImplemented
                        review.update({"date":date,"grade":grade})
                        yield (dict(review,**{"movie":movie}))
PRODUCER_ADDR=os.environ["PRODUCER_ADDR"]
session = requests.Session()
session.trust_env = False
gradeParser=GradeParser()
for obj in gradeParser.genMovieProcessed("/test/review_list"):
    try:
        res=session.post(url="http://"+PRODUCER_ADDR+"/review/push",json=obj)
        if not res.ok:
            raise Exception(res.status_code)
        print(res.json())
    except Exception as e:
        print("ERROR",e,PRODUCER_ADDR)
        # print("HEALTH_CHECK",requests.get())
    time.sleep(1) 