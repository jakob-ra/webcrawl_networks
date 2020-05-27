**Meeting 11.3.19**

- Michael contacts “dataprovider.com” and inquires whether it would be possible (and to what price) to obtain all websites of all Firms in Switzerland.

- Sebastian extends the webcrawling algorithm to save also pdf files. 

- Timo implements an algorithm that classifies websites and their content in relevant vs. irrelevant (using the output created by Sebastian’s webcrawling algorithm). We then organize a Skype meeting by the end of March (suggestion would be the 29th of March) to discuss the progress of this classification algorithm.

- After the meeting, and assuming that the classification algorithm by Timo works as expected, Timo starts the identification of the technology and product portfolios of the firms: 

(a) For the technology portfolios we use the Orbis-Patent IPC matches. We use both, supervised and unsupervised learning algorithms.

(b) For the product portfolios we use the Orbis primary and secondary industry codes. We use both, supervised and unsupervised learning algorithms.

(c) From the websites the locations of the headquarters of the firms are extracted.

When these algorithms are implemented and work as expected, Sebastian runs them on the KOF Survey data and tests them.

- The goal would be to create a database that is publicly accessible. The data will be summarized in a working paper (KOF, CEPR,…) so as to establish prior art. Ultimately, the data can be made accessible and publishes as "Nature Scientific Data” (with a reference to the continuously updated KOF website with the latest version of the data). 

Other issues we discussed in the medium run:

- Get a programmer to set up a website for the database that is maintained and updated over time. Further, buy servers to host the data.

- Extract information from the company’s website about their relationships (create a "knowledge graph"): 

(a) R&D collaborations: use Thomson SDC Platinum data as training set/test set.

(b) Buyer-supplier relationships: use Capital IQ Relationships data as training set/test set.

We can make the relational data freely accessible, whereas the above mentioned proprietary databases (Thomson Reuters or Capital IQ) are not publicly available, and too costly for most universities. Therefore there should be a big interest by the community in this work.

