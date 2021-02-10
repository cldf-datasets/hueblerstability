import pathlib

from cldfbench import CLDFSpec
from cldfbench import Dataset as BaseDataset
from pycldf import StructureDataset


class Dataset(BaseDataset):
    dir = pathlib.Path(__file__).parent
    id = "huebler"

    def cldf_specs(self):
        return CLDFSpec(
            dir=self.cldf_dir,
            module="StructureDataset",
            default_metadata_path=self.raw_dir / "StructureDataset-metadata.json",
        )

    def cmd_download(self, args):
        pass

    def cmd_makecldf(self, args):
        raw_ds = StructureDataset.from_metadata(self.raw_dir / "StructureDataset-metadata.json")
        args.writer.cldf.add_component("ExampleTable")

        for row in raw_ds["LanguageTable"]:
            args.writer.objects["LanguageTable"].append(row)

        for row in raw_ds["ParameterTable"]:
            args.writer.objects["ParameterTable"].append(row)

        for row in raw_ds["ValueTable"]:
            args.writer.objects["ValueTable"].append(row)

        for row in raw_ds["CodeTable"]:
            args.writer.objects["CodeTable"].append(row)

        for example in self.raw_dir.read_csv("examples.csv", dicts=True):
            args.writer.objects["ExampleTable"].append(
                dict(
                    ID=example["ID"],
                    Language_ID=example["Language_ID"],
                    Primary_Text=example["Primary_Text"],
                    Analyzed_Word=example["Analyzed_Word"],
                    Gloss=example["Gloss"],
                    Translated_Text=example["Translated_Text"],
                    Comment=example["Comment"],
                )
            )
