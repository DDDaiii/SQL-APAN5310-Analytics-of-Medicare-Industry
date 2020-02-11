# SQL-ETL-Pipeline-Analytics-of-Medicare-Industry
<img src="medicare.png">

<h1>Introduction</h1>
  
For our SQL final project, we were charged with the task of cleaning a messy dataset and transforming it into a live datasource that can support analytical applications. This project was meant to demonstrate the skills we learned throughout the semester and showcase our ability to support business decisions through data from non-uniform sources. The dataset we chose for this project was the Medicare Physician & Other Supplier NPI Aggregates data provided by the Centers for Medicare & Medicaid Services (CMS). (link provided below)
https://www.kaggle.com/cms/medicare-physician-other-supplier-npi-aggregates


<h1>Motivation & Research</h1>
We chose this dataset because it contains a significant amount of data (67 columns in the file, over 1 million rows) that could easily be split into more than 20 tables. Furthermore, this dataset is maintained by Socrata's API and Kaggle's API, meaning it is updated daily and allows for automated dashboard generation. 

<h1>Objectives and Background</h1>
<p>Medicare is a federal health insurance program for people of all ages with disabilities. According to KFF’s report “The Facts on Medicare Spending and Financing”, Medicare spending was $605 billion in 2018 and made up 15 percent of the federal budget. Moreover, with the growth of enrollment due to a larger aging population and the increase in healthcare prices per person, the future spending of Medicare is predicted to keep growing. According to the report “Medicare Plan Finder: Usability Problems and Incomplete Information Create Challenges for Beneficiaries Comparing Coverage Options” issued by the General Accounting Office, there is a lack of information about provider networks for beneficiaries before making their coverage decisions.</p>
<p>With a better understanding of payment data, government officials would be able to make wiser decisions regarding the refinement of current Medicare plans. Furthermore, beneficiaries would be able to make more educated decisions regarding their healthcare providers if given access to an organized Medicare database. This is our objective for our dataset, to create a database where providers and beneficiaries can have direct access to valuable information that will help them make educated decisions regarding coverage plans. We also want to provide an overview for government officials to observe Medicare usage data to help them determine strategies to revamp health care policies. This leads us to discuss our proposed scenario.</p>
