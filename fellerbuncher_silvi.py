#-------------------------------------------------------------------------------
# Name:         fellerbuncher.py
# Purpose:      Establish harvest areas using feller buncher data
#
# Author:       Mathew Coyle
#
# Created:      29/03/2012
# Copyright:    (c) Alberta-Pacific 2012
# Licence:      ArcInfo
#-------------------------------------------------------------------------------
#!/usr/bin/env python

# Import modules
import os
import datetime
import arcpy


def main():
    """ Main function to establish harvest areas of feller buncher.
    Requires feature class of blocks to analyze and feller buncher point data

    Standard parameters
    max_line = 10
    select_dist = "12 Meters"
    buffer_dist = "8 Meters"
    neg_buffer_dist = "-3 Meters"
    min_area = 1000
    xytol = "1 Meters"

    """
    #FB_ID = !SOURCEID! +"_"+ str(int( !Shape_Area!))

    # Import custom toolbox
    #arcpy.ImportToolbox(r"C:\GIS\tools\PointsToLines10\PointsToLines10.tbx")
    arcpy.ImportToolbox(r"\\millsite.net\filesystem\GISdata\gis\projects\coyle\Tools\PointsToLines10\PointsToLines10.tbx")

    # User variables
    max_line = 15  # maximum distance between points to join as connected path
    select_dist = "20 Meters"  # distance around block to assign points to it
    buffer_dist = "4 Meters"  # distance to buffer path
    neg_buffer_dist = "-1 Meters"  # distance to shrink from edges
    min_area = 100 # minimum area of holes to allow inside block
    xytol = "0.5 Meters"  # environment tolerance
    outName = "FINAL_HARVEST_US"

    # Set input data
    source_shp_dir = r"E:\feller_buncher_data\2012-13 Frozen"
    output = r"E:\feller_buncher_data\2013_02_11.gdb"  # output GDB
    scratch = r"C:\temp\scratch_fb.gdb"  # Scratch GDB only need folder
    inFeatures = "FB8920416821"  # blocks FC requires SOURCEID field

    # Set local variables
    fblayer = r"in_memory\fbtemplayer"
    lineField = ""
    sortField = "TIMETAG"
    sourceField = "SOURCEID"
    fbidField = "FB_ID"
    fb_fc = "fb_points_merged"
    fbidcode = "FB_CODE_ID"
    blocks_list = list()
    block_layer = r"in_memory\blocktemp"
    out_data = r"in_memory\output"
    temp_lyr = r"in_memory\temp"
    cur = None
    b = None
    upcur = None
    row = None

    def scratch_creation(scratch):
        """scratcb_creation(scratch)

        Create scratch geodatabase

        scratch(string)
        Path to geodatabase root"""

        scratch_create = True
        for i in range(0, 10):
            if scratch_create:
                scratch_return = "{0}{1}.gdb".format(scratch.split(".gdb")[0], i)
                if not arcpy.Exists(scratch_return):
                    arcpy.CreateFileGDB_management(
                        os.path.dirname(scratch_return),
                        os.path.basename(scratch_return))
                    print("Creating {0}".format(scratch_return))
                    scratch_create = False
                else:
                    print("Deleting {0}".format(scratch_return))
                    arcpy.Delete_management(scratch_return)
        return scratch_return

    # Environment settings

    if not arcpy.Exists(output):
        print("Source database not found")
    scratch = scratch_creation(scratch)
    print("Preparing data")
    arcpy.env.workspace = source_shp_dir  # input
    arcpy.env.scratchWorkspace = scratch
    arcpy.env.overwriteOutput = True
    arcpy.env.XYTolerance = xytol
    fbidFieldFound = False

    # Create list of input shapefiles
    fc_in_list = []
    shape_source_list = arcpy.ListFeatureClasses("*.shp", "Point")
    fb_field_status = "Status"
    fb_status = "WRK"

    fb_field_delim = arcpy.AddFieldDelimiters(
        shape_source_list[0], fb_field_status)

    for in_shape in shape_source_list:
        fb_base = in_shape.split(".")[0]
        out_temp_path = os.path.join(output, fb_base)

        arcpy.FeatureClassToFeatureClass_conversion(
            in_shape, output, fb_base,
            "{0} = '{1}'".format(fb_field_delim, fb_status))

        fc_in_list.append(fb_base)

        arcpy.AddField_management(
            out_temp_path, fbidcode,
            "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")

        upcur = arcpy.UpdateCursor(out_temp_path)
        for row in upcur:
            row.setValue(fbidcode, fb_base)
            upcur.updateRow(row)

    # Merge new input files
    arcpy.env.workspace = output
    arcpy.Merge_management(fc_in_list, fb_fc)

    # Check for FB_ID field in block layer, add and calculate if not found

    for field in arcpy.ListFields(inFeatures):
        if field.name == fbidField:
            fbidFieldFound = True
    if not fbidFieldFound:
        arcpy.AddField_management(
            inFeatures, fbidField,
            "TEXT", "", "", "25", "", "NULLABLE", "NON_REQUIRED", "")

        exp = "!SOURCEID!+'_'+str(int(!Shape_Area!))"

        arcpy.CalculateField_management(
            inFeatures, fbidField, exp, "PYTHON_9.3")

    # Build cursor to get list of blocks then delete cursor
    cur = arcpy.SearchCursor(inFeatures)
    for row in cur:
        blocks_list.append(row.getValue(fbidField))
    del cur

    # Build index of feller bunchers
    FBindex = list()
    indexcur = arcpy.SearchCursor(fb_fc, "", "", fbidcode, "%s A" % fbidcode)
    for row in indexcur:
        IDval = row.getValue(fbidcode)
        if IDval not in FBindex:
            FBindex.append(IDval)
    del indexcur

    # Loop through block list
    for b in blocks_list:
        print("\nProcessing {0}".format(b))

        where = "{0} = '{1}'".format(fbidField, b)

        arcpy.MakeFeatureLayer_management(inFeatures, block_layer, where)
        for feller in FBindex:
            print(feller)
            # can add in_memory when running output for perm
            b_path = os.path.join(
                scratch, "{0}{1}".format(b, feller))

            arcpy.MakeFeatureLayer_management(
                fb_fc, fblayer, "{0} = '{1}'".format(fbidcode, feller))

            arcpy.SelectLayerByLocation_management(
                fblayer, "WITHIN_A_DISTANCE", block_layer,
                select_dist, "NEW_SELECTION")

            selection = int(arcpy.GetCount_management(fblayer).getOutput(0))

            if selection != 0:
                print("{0} points for {1}".format(selection, feller))

                # Execute PointsToLine

                #arcpy.PointsToLine_management(fblayer, out_data, lineField, sortField)
                """
                Uncomment the previous line and comment out the next line if
                not using custom Points to Line tool.  This means the output
                may have errors from not using the max_line input.
                """
                arcpy.PointsToLinev10(
                    fblayer, out_data, lineField, sortField, "#", max_line)

                arcpy.MakeFeatureLayer_management(out_data, temp_lyr)

                arcpy.SelectLayerByLocation_management(
                    temp_lyr, "INTERSECT", block_layer, "#", "NEW_SELECTION")

                arcpy.Buffer_analysis(
                    temp_lyr,
                    "%s_buffer" % b_path,
                    buffer_dist,
                    "FULL", "ROUND", "ALL")

                # Double repair to ensure no errors
                arcpy.RepairGeometry_management(
                    "%s_buffer" % b_path, "DELETE_NULL")
                arcpy.RepairGeometry_management(
                    "%s_buffer" % b_path, "DELETE_NULL")

                arcpy.EliminatePolygonPart_management(
                    "%s_buffer" % b_path, "%s_eliminate" % b_path,
                    "AREA", min_area, "", "CONTAINED_ONLY")

                arcpy.RepairGeometry_management(
                    "%s_eliminate" % b_path, "DELETE_NULL")

                arcpy.AddField_management(
                    "%s_eliminate" % b_path, sourceField, "TEXT", "", "", "25")

                # Add SOURCEID to output feature
                upcur = arcpy.UpdateCursor("%s_eliminate" % b_path)
                for row in upcur:
                    #print "Setting {0} to {1}".format(sourceField, b.split("_")[0])  # temp error checking
                    row.setValue(sourceField, b.split("_")[0])
                    upcur.updateRow(row)

        #for feller in FBindex: Loop ended
    #for b in blocks_list: Loop ended

    print("\nProcessing final block areas")
    # Path to final output feature class
    final_output = os.path.join(output, outName)
    arcpy.env.workspace = scratch
    fcs_final = arcpy.ListFeatureClasses("*_eliminate")
    arcpy.Merge_management(fcs_final, "final_harvest_merge")

    arcpy.Union_analysis(
        "final_harvest_merge",
        "final_harvest_union",
        "NO_FID", xytol, "GAPS")

    #arcpy.Union_analysis(fcs_final, "final_harvest_union", "NO_FID", xytol, "GAPS")
    arcpy.Dissolve_management(
        "final_harvest_union", "final_harvest_dissolve",
        sourceField, "", "SINGLE_PART")

    # Eliminate doughnut holes below minimum area criterion
    arcpy.EliminatePolygonPart_management(
        "final_harvest_dissolve", "final_harvest_elim",
        "AREA", min_area, "", "CONTAINED_ONLY")

    # Negative buffer to compensate for ribbon line proximity
    if neg_buffer_dist != "0 Meters":
        arcpy.Buffer_analysis(
            "final_harvest_elim", final_output, neg_buffer_dist,
            "FULL", "ROUND", "LIST", sourceField)
    else:
        arcpy.FeatureClassToFeatureClass_conversion("final_harvest_elim", output, outName)

    arcpy.RepairGeometry_management(final_output, "DELETE_NULL")

if __name__ == "__main__":

    start_time = datetime.datetime.now()

    print("Start time {0}".format(start_time))

    # Call main function
    main()

    print("Elapsed time {0}".format(datetime.datetime.now() - start_time))
