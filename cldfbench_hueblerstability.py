import re
import pathlib
import collections

from cldfbench import CLDFSpec
from cldfbench import Dataset as BaseDataset
from pycldf import StructureDataset

REF_PATTERNS = [
    re.compile(r'(?P<author>[^(]+)\((?P<year>[^:)]+)(:(?P<pages>.*))?\)'),
    re.compile(r'(?P<author>[^\s]+)\s+(?P<year>[0-9]{4})'),
]


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "huebler"

    def cldf_specs(self):
        return CLDFSpec(
            dir=self.cldf_dir,
            module="StructureDataset",
        )

    def cmd_download(self, args):
        purge = [
        "TE001", "TE002", "TE009", "TE012", "TE014", "TE015", "TE016", "TE022", "TE025",
        "TE026", "TE028", "TE029", "TE033", "TE034", "TE036", "TE040", "TE041", "TE042",
        "TE043", "TE044", "TE045", "TE046", "TE047", "TE048", "TE049", "TE051", "TE055",
        "TE056", "TE057", "TE058", "TE060", "TE061", "TE062", "TE063", "TE064", "TE065",
        "TE067", "TE068", "TE069", "TE070", "TE071", "TE072", "TE073", "TE074", "TE076",
        "TE077", "TS081", "TS082", "TS083", "TS083", "TS084", "TS085"]
        
        pass

    def cmd_makecldf(self, args):
        self._schema(args.writer.cldf)
        glottolog = args.glottolog.api
        sources = {(r['author'], r['year']): r for r in self.etc_dir.read_csv('sources.csv', dicts=True)}
        #
        # FIXME: fetch bibtex for sources from Glottolog!
        #
        codes = collections.defaultdict(collections.Counter)
        missing = collections.Counter()
        COLS = [
            'Grambank ID',
            'Feature',
            'Possible Values',
            'Value',
            'Source',
            'Comment',
            'Detailed comment',
            'Example',
        ]
        pids = set()
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
                codes[val['Grambank ID']].update([val['Value']])
                assert val['Grambank ID'], str(val)
                if val['Grambank ID'] not in pids:
                    args.writer.objects['ParameterTable'].append(dict(
                        ID=val['Grambank ID'],
                        Name=val['Feature'],
                    ))
                    pids.add(val['Grambank ID'])
                for ref in val['Source'].split(';'):
                    ref = ref.strip()
                    if ref:
                        for p in REF_PATTERNS:
                            m = p.fullmatch(ref)
                            if m:
                                key = (m.group('author').strip(), m.group('year').strip())
                                if key not in sources:
                                    missing.update([key])
                                    print('---', ref)
                                break
                        else:
                            print(ref)
                args.writer.objects['ValueTable'].append(dict(
                    ID='{}-{}'.format(val['Grambank ID'], gc),
                    Value=val['Value'],
                    Language_ID=gc,
                    Parameter_ID=val['Grambank ID'],
                ))
        for k, v in missing.most_common():
            print(k, v)
        for code, vals in codes.items():
            if '' in vals:
                print(code, vals)
        return

    def _schema(self, cldf):
        cldf.add_columns(
            'ValueTable',
            {
                'name': 'Examples',
                'separator': ' ',
                'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#exampleReference'},
        )
        cldf.add_component('ExampleTable')
        cldf.add_component('LanguageTable')
        cldf.add_component('ParameterTable')
        cldf.add_component('CodeTable')
