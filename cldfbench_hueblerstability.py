import re
import pathlib
import itertools
import collections

import attr
from cldfbench import CLDFSpec
from cldfbench import Dataset as BaseDataset

REF_PATTERNS = [
    re.compile(r'(?P<author>[^(]+)\((?P<year>[^:)]+)(:(?P<pages>.*))?\)'),
    re.compile(r'(?P<author>[^\s]+)\s+(?P<year>[0-9]{4})'),
]


@attr.s
class Reference:
    author = attr.ib(converter=lambda s: s.strip())
    year = attr.ib(converter=lambda s: s.strip())
    pages = attr.ib(converter=lambda s: s.strip() if s else s, default=None)

    @property
    def key(self):
        return self.author, self.year

    def as_cldf(self, key):
        res = key
        if self.pages:
            res += '[{}]'.format(self.pages)
        return res


def match_ref(ref):
    ref = ref.strip()
    for p in REF_PATTERNS:
        m = p.fullmatch(ref)
        if m:
            return Reference(**m.groupdict())


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "huebler"

    def cldf_specs(self):
        return CLDFSpec(
            dir=self.cldf_dir,
            module="StructureDataset",
        )

    def cmd_download(self, args):
        pass

    def cmd_makecldf(self, args):
        self._schema(args.writer.cldf)
        glottolog = args.glottolog.api
        sources = {
            (r['author'], r['year']): r for r in self.etc_dir.read_csv('sources.csv', dicts=True)}
        examples = {eid: list(ex) for eid, ex in itertools.groupby(
            sorted(self.etc_dir.read_csv('examples.csv', dicts=True), key=lambda r: r['ID']),
            lambda r: r['ID']
        )}
        for eid, ex in examples.items():
            for i, e in enumerate(ex, start=1):
                e['ID'] = '{}-{}'.format(e['ID'], i)
        missing = collections.Counter()
        unknown = collections.Counter()
        pids, srcids, codes = set(), set(), collections.defaultdict(set)
        for sheet in self.raw_dir.glob('*.tsv'):
            m = re.fullmatch(r'(?P<name>[^\[]+)\[(?P<gc>[a-z0-9]{8})]', sheet.stem)
            lname, gc = m.group('name').strip(), m.group('gc')
            args.writer.objects['LanguageTable'].append(dict(
                ID=gc,
                Name=lname,
                Glottocode=gc,
            ))
            values = self.raw_dir.read_csv(sheet.name, delimiter='\t', dicts=True)
            assert values
            for val in values:
                assert val['Grambank ID'] and val['Value'], str(val)
                codes[val['Grambank ID']].add(val['Value'])
                if val['Grambank ID'] not in pids:
                    args.writer.objects['ParameterTable'].append(dict(
                        ID=val['Grambank ID'],
                        Name=val['Feature'],
                    ))
                    pids.add(val['Grambank ID'])

                refs = []
                for ref in val['Source'].split(';'):
                    reference = match_ref(ref)
                    if reference:
                        if reference.key not in sources:
                            missing.update([reference.key])
                        elif sources[reference.key]['matchid']:
                            refs.append(reference.as_cldf(sources[reference.key]['matchid']))
                            srcids.add(sources[reference.key]['matchid'])
                    else:
                        unknown.update([ref])

                args.writer.objects['ValueTable'].append(dict(
                    ID='{}-{}'.format(val['Grambank ID'], gc),
                    Value=val['Value'],
                    Language_ID=gc,
                    Parameter_ID=val['Grambank ID'],
                    Code_ID='{}-{}'.format(val['Grambank ID'], val['Value']) if val['Value'] != '?' else None,
                    Examples=[e['ID'] for e in examples.get('{}-{}'.format(gc, val['Grambank ID']), [])],
                    Source=refs,
                ))
        for pid, vals in sorted(codes.items()):
            for val in vals:
                if val != '?':
                    args.writer.objects['CodeTable'].append(dict(
                        ID='{}-{}'.format(pid, val),
                        Parameter_ID=pid,
                        Name=val,
                    ))
        misligned = 0
        for exs in examples.values():
            for ex in exs:
                if not ex['Primary_Text']:
                    ex['Primary_Text'] = ex['Analyzed_Word']
                ex['Analyzed_Word'] = ex['Analyzed_Word'].split()
                ex['Gloss'] = ex['Gloss'].split()
                if not ex['Gloss']:
                    ex['Gloss'] = None
                if ex['Gloss'] and len(ex['Analyzed_Word']) != len(ex['Gloss']):
                    print(ex['ID'])
                    print(ex['Analyzed_Word'])
                    print(ex['Gloss'])
                    misligned += 1
                args.writer.objects['ExampleTable'].append(ex)
        print(misligned)
        for srcid in sorted(srcids):
            src = glottolog.bibfiles[srcid]
            src.key = srcid
            args.writer.cldf.add_sources(str(src))

        for k, v in missing.most_common():
            print(k, v)
        return

    def _schema(self, cldf):
        cldf.add_columns(
            'ValueTable',
            {
                'name': 'Examples',
                'separator': ' ',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#exampleReference'},
        )
        cldf['ValueTable', 'value'].null = ['?']
        cldf.add_component('ExampleTable')
        cldf.add_component('LanguageTable')
        cldf.add_component('ParameterTable')
        cldf.add_component('CodeTable')
