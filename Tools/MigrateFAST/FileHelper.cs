namespace ExcelToTMDL.Utils
{
	public static class FileHelper
	{
		public static void EnsureDirectory(string path)
		{
			if (!Directory.Exists(path))
			{
				Directory.CreateDirectory(path);
			}
		}
	}
}
