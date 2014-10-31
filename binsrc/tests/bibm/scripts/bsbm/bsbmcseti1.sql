

rdf_cset ('product', properties => vector (
'http://www.w3.org/2000/01/rdf-schema#label',
'http://purl.org/dc/elements/1.1/date',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric1',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric2',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual1',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual2',
'http://purl.org/dc/elements/1.1/publisher',
'http://www.w3.org/2000/01/rdf-schema#comment',
vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/producer', 'index'),
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric3',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual3',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual5',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric5',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual4',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric4',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyNumeric6',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productPropertyTextual6'),
types => vector ());

cset_iri_pattern ('product', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer/Product%%', vector (vector (0, 100000000000)));



rdf_cset ('review', properties => vector (
'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
'http://purl.org/dc/elements/1.1/date',
'http://purl.org/dc/elements/1.1/publisher',
vector ('http://purl.org/stuff/rev#reviewer', 'index'), 
'http://purl.org/stuff/rev#text', 
vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewFor', 'index'), 
vector ('http://purl.org/dc/elements/1.1/title'), 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/reviewDate', 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating4', 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating2',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating3', 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/rating1'),
types => vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Review'))
;


cset_iri_pattern ('review', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite/Review%%', vector (vector (0, 100000000000)));

rdf_cset ('offer', properties => vector (
'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/price',
'http://purl.org/dc/elements/1.1/date', 
vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/product', 'index'), 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validFrom',
vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/vendor', 'index'), 
vector ('http://purl.org/dc/elements/1.1/publisher', 'index'), 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/deliveryDays', 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/offerWebpage',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/validTo'),
types => vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Offer'));


cset_iri_pattern ('offer', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor/Offer%%', vector (vector (0, 200000000000)));

rdf_cset ('person', properties => vector (
vector ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'index'), 
'http://purl.org/dc/elements/1.1/date', 
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country',
'http://purl.org/dc/elements/1.1/publisher', 
'http://xmlns.com/foaf/0.1/mbox_sha1sum',
'http://xmlns.com/foaf/0.1/name'),
types => vector ('http://xmlns.com/foaf/0.1/Person'))
;

cset_iri_pattern ('person', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromRatingSite/Reviewer%%', vector (vector (0, 10000000000)));

rdf_cset ('vendor', properties => vector (
vector ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'index'), 
'http://www.w3.org/2000/01/rdf-schema#label',
'http://purl.org/dc/elements/1.1/date',
vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country', 'index'),
vector ('http://purl.org/dc/elements/1.1/publisher', 'index'),
'http://www.w3.org/2000/01/rdf-schema#comment',
'http://xmlns.com/foaf/0.1/homepage'),
types => vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Vendor'))
;

cset_iri_pattern ('vendor', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor/Vendor%%', vector (vector (0, 10000000000)));

rdf_cset ('producer', properties => vector (
vector ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'index'),
'http://purl.org/dc/elements/1.1/publisher',
'http://www.w3.org/2000/01/rdf-schema#label',
'http://xmlns.com/foaf/0.1/homepage',
'http://purl.org/dc/elements/1.1/date',
'http://www.w3.org/2000/01/rdf-schema#comment',
'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/country'),
types => vector ('http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/Producer'));

cset_iri_pattern ('producer', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer/Producer%%', vector (vector (0, 1000000000)));



xml_set_ns_decl ('foaf',  'http://xmlns.com/foaf/0.1/', 2);
xml_set_ns_decl ('rev',  'http://purl.org/stuff/rev#', 2);
xml_set_ns_decl ('bsbm', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/', 2);
xml_set_ns_decl ('bsbm-inst', 'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/', 2);

