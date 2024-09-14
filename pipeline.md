```mermaid
%%{ init: { 'flowchart': { 'curve': 'monotoneX' } } }%%
graph LR;
producer[fa:fa-rocket producer &#8205] --> stocks{{ fa:fa-arrow-right-arrow-left stocks &#8205}}:::topic;
stocks{{ fa:fa-arrow-right-arrow-left stocks &#8205}}:::topic --> anomalydetector[fa:fa-rocket anomalydetector &#8205];
anomalydetector[fa:fa-rocket anomalydetector &#8205] --> anomalies{{ fa:fa-arrow-right-arrow-left anomalies &#8205}}:::topic;


classDef default font-size:110%;
classDef topic font-size:80%;
classDef topic fill:#3E89B3;
classDef topic stroke:#3E89B3;
classDef topic color:white;
```