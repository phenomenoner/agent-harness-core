import json
import pytest
from ahc.infrastructure.legacy_memory_classification import classify_legacy_record


def classify(table,row,**kwargs):
    return classify_legacy_record({'table':table,'row':row},owner='main',
        scope='agent-harness-core/main',**kwargs)


def test_document_source_admission_is_explicit_and_does_not_guess_owner_from_path():
    row={'path':'main/notes.md','repo':'main','text':'Preserved user knowledge','embedding':{'sqlite_blob_base64':'AAAA'}}
    assert classify('chunks',row)['disposition']=='blocked'
    result=classify('chunks',row,document_source_admitted=True)
    assert result['disposition']=='serve'
    assert result['observation']['summary']==row['text']
    assert 'embedding' not in result['observation']['detail']['legacy_document']
    assert row['embedding']=={'sqlite_blob_base64':'AAAA'}


@pytest.mark.parametrize('owner,expected',[('main','serve'),('unknown','blocked'),('xiaoxiaoli','archive')])
def test_global_event_scope_still_requires_exact_owner(owner,expected):
    row={'agent_id':owner,'scope':'global','type':'conversation.user',
         'summary':'Historical request','payload_json':'{"text":"historical data"}'}
    assert classify('episodic_events',row,event_scopes=('global',))['disposition']==expected
    assert classify('episodic_events',row)['disposition']!='serve'


@pytest.mark.parametrize('raw',['[]','["historical",1]','null','true','"historical"','42'])
def test_known_event_owner_does_not_depend_on_payload_object_shape(raw):
    row={'agent_id':'main','scope':'global','type':'conversation.assistant',
         'summary':'Historical answer','payload_json':raw}
    result=classify('episodic_events',row,event_scopes=('global',))
    assert result['disposition']=='serve'
    assert result['observation']['detail']['legacy_event']['payload_json']==raw


@pytest.mark.parametrize('raw',[None,'{','NaN','{"a":1,"a":2}'])
def test_known_event_owner_can_serve_text_with_malformed_inert_payload(raw):
    row={'agent_id':'main','scope':'global','summary':'Historical answer','payload_json':raw}
    result=classify('episodic_events',row,event_scopes=('global',))
    assert result['disposition']=='serve'
    assert result['observation']['detail']['legacy_event']['payload_json']==raw
    assert result['observation']['detail']['payload_format']==(
        'absent' if raw is None else 'invalid-json-retained-verbatim')
    row['agent_id']='unknown'
    assert classify('episodic_events',row,event_scopes=('global',))['disposition']=='blocked'


@pytest.mark.parametrize('detail',[
    {'agent_id':'other','scope':'agent-harness-core/main'},
    {'agent_id':'main','scope':'other'},
    {'agent_id':'main','agentId':'other'},
])
def test_document_admission_never_overrides_explicit_foreign_or_conflicting_owner(detail):
    result=classify('observations',{'kind':'note','summary':'historical',
        'detail_json':json.dumps(detail)},document_source_admitted=True)
    assert result['disposition']!='serve'


def test_camel_case_owner_and_mapped_ownerless_document_are_supported():
    row={'kind':'note','summary':'Full text','detail_json':'{"agentId":"main"}'}
    assert classify('observations',row)['disposition']=='serve'
    row['detail_json']='{"file_hash":"captured","source_path":"legacy-document.md"}'
    assert classify('observations',row)['disposition']=='blocked'
    assert classify('observations',row,document_source_admitted=True)['disposition']=='serve'


@pytest.mark.parametrize('raw',['{"agent_id":"main","agent_id":"other"}','{"agent_id":"main","value":NaN}'])
def test_malformed_ownership_does_not_supply_admission(raw):
    assert classify('observations',{'kind':'note','summary':'text','detail_json':raw},
        document_source_admitted=True)['disposition']=='blocked'


def test_large_mapped_record_requires_explicit_split_and_never_silently_truncates():
    text='x'*65000
    result=classify('docs_chunks',{'text':text},document_source_admitted=True)
    assert result['disposition']=='blocked'
    assert text=='x'*65000


def test_metadata_is_preserved_as_archive_and_not_a_semantic_memory():
    assert classify('files',{'path':'source.md','hash':'hash'})['disposition']=='archive'
