namespace RelationshipTmdlGeneratorUI
{
    class Relationship
    {
        public string FromTable { get; set; } = "";
        public string FromColumn { get; set; } = "";
        public string ToTable { get; set; } = "";
        public string ToColumn { get; set; } = "";
        public string Cardinality { get; set; } = "ManyToOne";
        public string CrossFilteringBehavior { get; set; } = "Single";
    }
}
