

import os


class MakeDirs(object):

    def mk_dir(self):

        folders = {
            "raw_data": ["GeoSheet", "source_ancillary"],
            "processing": ["geographic", "ancillary"],
            "merged_file": ["geographic"]
        }

        for folder in folders.keys():

            fdir = "%s" % (folder)

            if not os.path.isdir(fdir):

                os.mkdir(fdir)

                for subfolder in folders[folder]:

                    subfdir = "%s/%s" % (folder, subfolder)

                    if not os.path.isdir(subfdir):

                        os.mkdir(subfdir)

                    else:

                        pass

            else:

                for subfolder in folders[folder]:

                    subfdir = "%s/%s" % (folder, subfolder)

                    if not os.path.isdir(subfdir):

                        os.mkdir(subfdir)

                    else:

                        pass

        return