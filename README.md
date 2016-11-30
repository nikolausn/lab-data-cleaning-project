\chapter{A generic framework to automate parts of data cleaning}
Even though we managed to analyse the data using OpenRefine, doing cleaning operation using OpenRefine. Firstly, the \textit{Traffic Violation} dataset is to large, with more than 800,000 rows,for OpenRefine common transform and merging process. Therefore, every time we do cleanup operation, the page become unresponsive because it waits for the iteration to finish. Next, the 5GB memory that we have given to OpenRefine runs out easily because the dataset is preloaded into the memory. Every step in our cleaning process will dramatically increase memory consumption which will meet the limitation in the maximum memory that we can use in some point. Finally, doing cleaning operations manually will be tedious since the dataset contains 35 columns in total. In addition, based on the analysis that we have made, the cleanup operations are mostly the same for some fields. 

\begin{figure}[!htbp]
    \centering
    \includegraphics[scale=0.60]{figures/OpenRefineUnresponsive}
    \caption{Unresponsive cleaning operation using OpenRefine}
    \label{fig:OpenRefineUnresponsive}
\end{figure} \break

Based on the problems that we faced with OpenRefine we decided to build an inhouse data cleaning program which can do data cleaning operations just like OpenRefine but consume less memory and can automate our cleaning process. First of all, we build the framework using \textit{Python} as text processing tool. Next the data cleaning framework use case are:
\begin{itemize}
  \item can do common transform for Trim, Upper, Lower, Regular Expression Replace, Collapse Whit Space Characters
  \item can merge values in some fields as an input for OpenRefine cluster and merge process
  \item can do merging Operation by reading OpenRefine extracted configuration json file and apply the replaced value to the dataset
  \item can upload the cleaned values into SQLite Database
\end{itemize}
Finally, we named the tool as Data Cleaning Framework (DCF).

\section{Data Cleaning Framework (DCF)}
DCF features is shown in following \ref{fig:DCF} figure
\begin{figure}[!htbp]
    \centering
    \includegraphics[scale=0.60]{figures/DCF}
    \caption{DCF features}
    \label{fig:DCF}
\end{figure} \par
descriptions of each features are:
\begin{enumerate}
    \item Common Transform: reading input file, applying common transform operation to particular fields in the dataset and write new output cleaned file.
    \begin{enumerate}
        \item upper: transform the fields into upper case characters value
        \item lower: transform the fields into lower case characters value
        \item trim: erase / trim whitespace characters in left side or right side of the value on some fields
        \item colspace: collapse white space characters (more than one space) inside the fields value
        \item regexrep: replace value in a field that match regular expression pattern into other characters
    \end{enumerate}
    \item Aggregate: select some fields, group and count values in a field, merge all aggregated values in each predefined fields into one file.
    \begin{enumerate}
        \item select: select particular fields defined from parameter into new output file.
        \item group: group and count values in each fields defined in parameter and output the aggregated value into separated output files based on the field names.
        \item merge: aggregate values in predefined fields and combine those aggregated values into one small output file. This output file can be used for OpenRefine cluster and merge operation.
    \end{enumerate}
    \item Automated Process: run cleanup operation based on predefined configuratin
    \begin{enumerate}
        \item init: read DCF json configuration file and apply cleanup operation based on the step defined in the configuration.
        \item massedit: read OpenRefine extracted json configuration file and apply openrefine massedit operation to the dataset.
    \end{enumerate}
    \item SQL Operation: load the dataset into database and perform select query from the database;
    \begin{enumerate}
        \item loadtable: read csv dataset and directly load the dataset into SQLite database file.
        \item runquery: read file that contains sql query and perform select query to the database defined in parameter and output the result into new csv dataset.
    \end{enumerate}
\end{enumerate}

\section{DCF Workflows}
\subsection{Main Workflow}
Main / core workflow of the DCF tool is describe in the following \ref{fig:MainWorkflow}
\begin{figure}[!htbp]
    \centering
    \includegraphics[scale=0.60,angle=90]{figures/MainWorkflow}    
    \caption{DCF's main workflow}
    \label{fig:MainWorkflow}
\end{figure} \par
In general, DCF will read parameters defined by user and parse the parameters value using parser configuration. One parameter defines input file which contains dataset that want to be cleaned. In addition the parameters also contain operation name parsed by predefined parser which will determine which operation will be executed by DCF. Based on the operation name, parser will custom the required and optional parameters needed to perform the operation. Finally, the result of the operations will be compiled in output file which can be a single output file, database file, or many aggregated files based on the operation.

\subsection{Init Cleanup Workflow}
Init operation workflow of DCF tool is describe in the following \ref{fig:InitConfigWorkflow}
\begin{figure}[!htbp]
    \centering
    \includegraphics[scale=0.60]{figures/InitConfigWorkflow}   
    \caption{DCF's init configuration workflow}
    \label{fig:InitConfigWorkflow}
\end{figure} \par

In this workflow DCF will read input file and init configuration file following DCF's configuration format. Firstly, DCF will do horizontal cleaning process in which the tool reads the input file using input stream. For each row, DCF will perform cleanup operation defined in the configuration file following the sequence ordered in hcleaning field array. After performing the cleaning operation DCF will write the cleaned row into output file stream which will make horizontal\_cleaned\_file output. This horizontal cleaning operation is best used for Common Transform operations. Next, after horizontal cleaning file created, DCF will do vertical cleaning in which DCF will read particular selected fields to be cleaned and load the fields into memory. DCF then will read vertical\_config operation from vcleaning field array from configuration file and do the operations defined sequentially. This vertical cleaning operation is best used for spliting and joining fields or any other custom operations that require or depend to the other values in different rows. \par

The structure for DCF's json config file is describe in the following tree:
\dirtree{%
.1 dcf's json configuration file.
.2 hcleaning: perform horizontal cleaning.
.3 field: field name.
.3 newfield: new cleanse field name.
.3 operation: array of operations, dcf will execute the operations in order.
.4 fname: function name (regexReplace,trim,collapseWhiteSpace,toUpper,toLowe,custom).
.4 fparam: additional parameter needed for regex replace and custom function.
.5 regexparam: additional parameter for regex replace operation.
.6 regex: regular expression pattern search value.
.6 replace: value to replace the match string.
.4 fcustom: boolean value to define that the operation perform custom command.
.4 file: if fcustom true then this attribut must define python file (without .py) to be included automatically in the DCF as an external resource.
.4 module: function name to be called from the python resource.
.2 vcleaning: perform vertical cleaning operations.
.3 field: field to be cleaned.
.3 operation: array of vertical cleaning operations, dcf will execute the operations in order, this array perform mostly custom operation.
}

\section{Cleaning Operation Workflow}
To clean Traffic Violation dataset we build a batch script containing commands to perform cleaning operation using DCF following this figure \ref{fig:BatchScriptWorkflow}
\begin{figure}[!htbp]
    \centering
    \includegraphics[scale=0.75]{figures/BatchScriptWorkflow}   
    \caption{Batch script cleaning workflow}
    \label{fig:BatchScriptWorkflow}
\end{figure} \par
Firstly, the script will call DCF's id operation to add ROWID on the dataset, this ROWID is used for querying the provenance to understand which row that has been cleaned following the particular unique ID. This operation will make prodTrafficId file. Next, the script will call DCF's init operation using defined init configuration file. This configuration file is build based on our previous clean up analysis on dataset and produce prodTraffic1 file. The prodTraffic1 file will become input for DCF's merge operation on selected "Model 1,Make 1,Description 1,Driver City 1,Location 1" fields and produce prodTrafficMerge file. The fields selected are fields that will be clustered and merge. 
\par
Using OpenRefine, we open the prodTrafficMerge file and perform cluster and merge operation. This operation is a manual tedious operation in which we must select carefully which value that we want to be clustered and merged. After we done with the OpenRefine's merging operation, we extract the OpenRefine json configuration for this merging operation only and save it as openrefinceconfig.json.
\par
Back again using DCF tool, we use openrefineconfig.json as a configuration input for DCF's massedit operation to prodTraffic1 dataset. This operation will read json config value that has mass-edit command and perform value rewriting if the value has matched with suggested values in openrefineconfig.json. This massedit process will produce prodTraffic2 file. This prodTraffic2 file contains both old fields and cleansed field, therefore, we need to use DCF's select operation to select just cleansed fields in order for loading process to database and produce prodTraffic3 file.
\par
Finally we load the prodTraffic3 file into sqlite database using DCF's loadtable operation producing prodTraffic.db file. In addition, from the cleansed prodTraffic3 file we perform group operation for fields SubAgency,Property Damage,State,VehicleType 2,Violation Type,Article,Race,Gender,Arrest Type 2. These fields are fields that we want to perform look-up value operation. Based on the aggregated values we can collect dictionaries of the fields. The look-up files then are loaded one by one into generated prodTraffic.db database.

\section{The Provenance}
Every operations in DCF that change value in the dataset will produce log files which we can used to trace the provenance of our cleaning operations. The log is a csv file contains these attributes:
\begin{itemize}
 \item id: ID that has changed, if rowid is not present in operation parameter then it will use line number
\item field: field that affected by cleanup operation
\item timestamp: local time in which the clean operation performed
\item function: which clenup operation used 
\item oldval: old value before cleaning
\item newval: new value after cleaning operation completed
\end{itemize}
After the log file created we can load the data into sqlite database cleansed file and check the log using sql command to aggregate or trace particular id like the example in figure \ref{fig:ProvenanceQuery} to summarize our cleanup operations.
\begin{figure}[!htbp]
    \centering
    \includegraphics[scale=0.60]{figures/ProvenanceQuery}   
    \caption{Clean up summary}
    \label{fig:ProvenanceQuery}
\end{figure}
