# Project Instructions for AI agents

By working only on the contents of the `sec_json_tools` directory you must drive the execution time of the command `python3 main.py` low enough, so that the number it prints on its standard output is higher than 2. 

## MANDATORY instructions

1. You must read the file `.gitignore`
2. You must read the file `.aiexclude`
3. You must read `README.md`
4. You are EXPLICITLY FORBIDDEN to modify `main.py`!
5. You MUST NEVER modify ANY files in the repository root.
6. You MUST NEVER perform any git operations. These are reserved exclusively for the user.

## Hints

1. The reason for the restrictions in `.aiexclude` specifically is that the project will operate on a large and diverse dataset. Basing any analysis or optimization strategy on the contents of concrete test data files will lead to over-fitting and produce a solution that does not generalize to the entire set of data.
2. This development system has completely functional installations of the GNU GCC compiler and of all python modules, listed in the `requirements.txt` file. You are encouraged to consider using them to achieve the project's goal. The preferred solution is python-only, but any mix of python, pure C and ubiquitous GNU/Linux CLI commands is also acceptable.
3. You are strongly discouraged to use other programming languages, modules, libraries and so, but if doing so would bring significant performance gains, you can ask the user for an exemption.
4. You are strongly encouraged to create new artifacts only under the `sec_json_tools` directory. If the user explicitly approves, you may create new artifacts in the project root, too.
5. The command `egrep -o '"Assets":{[^]]*\]}}' CIK0000001750.json` returns the following:
```
"Assets":{"label":"Assets","description":"Sum of the carrying amounts as of the balance sheet date of all assets that are recognized. Assets are probable future economic benefits obtained or controlled by an entity as a result of past transactions or events.","units":{"USD":[{"end":"2024-08-31","val":2783300000,"accn":"0001410578-24-001617","fy":2025,"fp":"Q1","form":"10-Q","filed":"2024-09-24","frame":"CY2024Q3I"},{"end":"2024-11-30","val":2849300000,"accn":"0001410578-25-000003","fy":2025,"fp":"Q2","form":"10-Q","filed":"2025-01-08","frame":"CY2024Q4I"},{"end":"2025-02-28","val":2859100000,"accn":"0001410578-25-000519","fy":2025,"fp":"Q3","form":"10-Q","filed":"2025-03-28","frame":"CY2025Q1I"},{"end":"2025-05-31","val":2844600000,"accn":"0001410578-25-001475","fy":2025,"fp":"FY","form":"10-K","filed":"2025-07-22"},{"end":"2025-05-31","val":2844600000,"accn":"0001104659-25-092589","fy":2026,"fp":"Q1","form":"10-Q","filed":"2025-09-23"},{"end":"2025-05-31","val":2844600000,"accn":"0001104659-26-001420","fy":2026,"fp":"Q2","form":"10-Q","filed":"2026-01-07"},{"end":"2025-05-31","val":2844600000,"accn":"0001104659-26-033973","fy":2026,"fp":"Q3","form":"10-Q","filed":"2026-03-25","frame":"CY2025Q2I"},{"end":"2025-08-31","val":2929700000,"accn":"0001104659-25-092589","fy":2026,"fp":"Q1","form":"10-Q","filed":"2025-09-23","frame":"CY2025Q3I"},{"end":"2025-11-30","val":3242500000,"accn":"0001104659-26-001420","fy":2026,"fp":"Q2","form":"10-Q","filed":"2026-01-07","frame":"CY2025Q4I"},{"end":"2026-02-28","val":3332500000,"accn":"0001104659-26-033973","fy":2026,"fp":"Q3","form":"10-Q","filed":"2026-03-25","frame":"CY2026Q1I"}]}}
```
From this code block you can see a typical record for each of the known facts and for each of the unwanted keys.
